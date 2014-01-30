package log

import (
	"errors"
	kinesis "github.com/sendgridlabs/go-kinesis"
	"log"
	"sync"
	"time"
)

type BufferedKinesisWriter struct {
	messages   chan string
	client     *kinesis.Kinesis
	streamName string
	flushLock *sync.Mutex
}

func NewBufferedKinesisWriter(accessKey, secretKey, streamName string, buffer int) *BufferedKinesisWriter {
	writer := new(BufferedKinesisWriter)
	writer.client = kinesis.New(accessKey, secretKey)
	writer.messages = make(chan string, buffer)
	writer.streamName = streamName
	writer.flushLock = new(sync.Mutex)
	return writer
}

// send sends a data blob to Kinesis.
func (writer *BufferedKinesisWriter) send(message string) error {
	// _, err := writer.client.PutRecord()
	return nil
}

// Flush writes all messages that have been buffered thus far to the
// Kinesis stream.
func (writer *BufferedKinesisWriter) Flush() error {
	// First, get the messages off of the buffer, so that we don't tie
	// up other processes too long
	writer.flushLock.Lock()
	var (
		bufferedMessages = make([]string, len(writer.messages))
	)
	for i := 0; i < len(writer.messages); i++ {
		bufferedMessages[i] = <-writer.messages
	}
	writer.flushLock.Unlock()

	// Now, send all the buffered messages
	for _, message := range bufferedMessages {
		if err := writer.send(message); err != nil {
			return err
		}
	}
	return nil
}

// Write matches io.Writer and will write the provided data.  If the
// buffer is length 0, it will actually send the data directly to
// Kinesis synchronously; otherwise, it will add data to the writer's
// buffer.  When the buffer is full, Flush() is called in a separate
// goroutine to start sending the buffered messages to Kinesis, before
// the passed in data is added to the buffer.
func (writer *BufferedKinesisWriter) Write(data []byte) (int, error) {
	return writer.write(data, true)
}

func (writer *BufferedKinesisWriter) write(data []byte, recurse bool) (int, error) {
	if cap(writer.messages) == 0 {
		return len(data), writer.send(string(data))
	}
	select {
	case writer.messages <- string(data):
	default:
		go writer.Flush()

		// Try to directly send the message now, but don't wait too
		// long.  If the request times out, try one recursive call
		// before giving up.
		select {
		case writer.messages <- string(data):
		case <-time.After(50 * time.Millisecond):
			if recurse {
				return writer.write(data, false)
			}
			return 0, errors.New("Send failed: timed out.")
		}
	}
	return len(data), nil
}

func (writer *BufferedKinesisWriter) Close() error {
	err := writer.Flush()
	close(writer.messages)
	return err
}

type KinesisLogger struct {
	log.Logger
	writer *BufferedKinesisWriter
}

// Close flushes all remaining messages in the logger to kinesis and
// closes the message channel.
func (logger *KinesisLogger) Close() error {
	return logger.writer.Close()
}

// New creates a new *KinesisLogger.  The parameters are used as
// follows:
//
// accessKey and secretKey are used for connecting to kinesis.
// streamName is used as the stream to send messages to when a log
//   message is written.
// logPrefix and flag are used for creating the *log.Logger.
// buffer is used to buffer the messages that will be sent to kinesis.
func NewLogger(accessKey, secretKey, streamName, logPrefix string, flag, buffer int) *KinesisLogger {
	writer := NewBufferedKinesisWriter(accessKey, secretKey, streamName, buffer)

	baseLogger := log.New(writer, "", flag)
	return &KinesisLogger{Logger: *baseLogger, writer: writer}
}
