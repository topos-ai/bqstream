package bqstream

import (
	"context"
	"fmt"
	"io"

	storage "cloud.google.com/go/bigquery/storage/apiv1beta1"
	"github.com/googleapis/gax-go/v2"
	"github.com/linkedin/goavro"
	storagepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta1"
	"google.golang.org/grpc"
)

type BigQueryStreamRequest struct {
	Project   string
	Dataset   string
	Table     string
	Parent    string
	Streams   int32
	Processor func(row map[string]interface{}) error
}

func BigQueryStream(ctx context.Context, req *BigQueryStreamRequest) error {
	c, err := storage.NewBigQueryStorageClient(ctx)
	if err != nil {
		return err
	}

	defer c.Close()

	tableReference := &storagepb.TableReference{
		ProjectId: req.Project,
		DatasetId: req.Dataset,
		TableId:   req.Table,
	}

	createReadSessionRequest := &storagepb.CreateReadSessionRequest{
		TableReference:   tableReference,
		RequestedStreams: req.Streams,
		Parent:           fmt.Sprintf("projects/%s", req.Project),
	}

	session, err := c.CreateReadSession(ctx, createReadSessionRequest)
	if err != nil {
		return err
	}

	if len(session.GetStreams()) == 0 {
		return fmt.Errorf("no streams in session")
	}

	// Add a cancel context.
	ctx, cancel := context.WithCancel(ctx)

	// The readers write to a row channel.
	ch := make(chan *storagepb.AvroRows)

	// Read using all available streams.
	readStreams := session.GetStreams()
	processStreamErrs := make([]chan error, len(readStreams))
	for i, readStream := range readStreams {
		processStreamErrs[i] = make(chan error)
		go func(readStream *storagepb.Stream, errs chan error) {
			defer close(errs)
			if err := processStream(ctx, c, readStream, ch); err != nil {
				errs <- err
			}
		}(readStream, processStreamErrs[i])
	}

	errs := make(chan error)
	go func() {
		defer close(errs)
		if err := processAvro(ctx, session.GetAvroSchema().GetSchema(), ch, req.Processor); err != nil {
			errs <- err
		}
	}()

	var streamErr error
	for _, errs := range processStreamErrs {
		for streamErr = range errs {
			cancel()
		}
	}

	close(ch)
	for streamErr = range errs {
	}

	cancel()
	return streamErr
}

// rpcOpts is used to configure the underlying gRPC client to accept large
// messages. The BigQuery Storage API may send message blocks up to 10MB
// in size.
var rpcOpts = gax.WithGRPCOptions(
	grpc.MaxCallRecvMsgSize(1024 * 1024 * 11),
)

// processStream reads rows from a single storage Stream, and sends the Avro
// data blocks to a channel. This function will retry on transient stream
// failures and bookmark progress to avoid re-reading data that's already been
// successfully transmitted.
func processStream(ctx context.Context, client *storage.BigQueryStorageClient, st *storagepb.Stream, ch chan<- *storagepb.AvroRows) error {
	var offset int64
	streamRetry := 3

	for {

		// Send the initiating request to start streaming row blocks.
		rowStream, err := client.ReadRows(ctx, &storagepb.ReadRowsRequest{
			ReadPosition: &storagepb.StreamPosition{
				Stream: st,
				Offset: offset,
			}}, rpcOpts)
		if err != nil {
			return fmt.Errorf("couldn't invoke ReadRows: %v", err)
		}

		// Process the streamed responses.
		for {
			r, err := rowStream.Recv()
			if err == io.EOF {
				return nil
			}

			if err != nil {
				streamRetry--
				if streamRetry <= 0 {
					return fmt.Errorf("processStream retries exhausted: %v", err)
				}

				break
			}

			if rc := r.GetRowCount(); rc > 0 {

				// Bookmark our progress in case of retries and send the rowblock on the channel.
				offset = offset + rc
				ch <- r.GetAvroRows()
			}
		}
	}
}

// processAvro receives row blocks from a channel, and uses the provided Avro
// schema to decode the blocks into individual row messages for printing. Will
// continue to run until the channel is closed or the provided context is
// cancelled.
func processAvro(ctx context.Context, schema string, ch <-chan *storagepb.AvroRows, processor func(map[string]interface{}) error) error {

	// Establish a decoder that can process blocks of messages using the
	// reference schema. All blocks share the same schema, so the decoder
	// can be long-lived.
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return fmt.Errorf("couldn't create codec: %v", err)
	}

	for {
		select {
		case <-ctx.Done():

			// Context was cancelled. Stop.
			return nil
		case rows, ok := <-ch:
			if !ok {

				// Channel closed, no further avro messages. Stop.
				return nil
			}

			undecoded := rows.GetSerializedBinaryRows()
			for len(undecoded) > 0 {
				datum, remainingBytes, err := codec.NativeFromBinary(undecoded)
				if err == io.EOF {
					break
				}

				if err != nil {
					return fmt.Errorf("decoding error with %d bytes remaining: %v", len(undecoded), err)
				}

				typeMaps, ok := datum.(map[string]interface{})
				if !ok {
					return fmt.Errorf("failed type assertion: %v", datum)
				}

				values := map[string]interface{}{}
				for key, typeMapInterface := range typeMaps {
					typeMap, ok := typeMapInterface.(map[string]interface{})
					if !ok {
						continue
					}

					for _, value := range typeMap {
						values[key] = value
						break
					}
				}

				if err := processor(values); err != nil {
					return err
				}

				undecoded = remainingBytes
			}
		}
	}
}
