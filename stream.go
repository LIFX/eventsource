package eventsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Stream handles a connection for receiving Server Sent Events.
// It will try and reconnect if the connection is lost, respecting both
// received retry delays and event id's.
type Stream struct {
	c           http.Client
	req         *http.Request
	lastEventId string
	retry       time.Duration
	closed      bool
	// Events emits the events received by the stream
	Events chan Event
	// Errors emits any errors encountered while reading events from the stream.
	// It's mainly for informative purposes - the client isn't required to take any
	// action when an error is encountered. The stream will always attempt to continue,
	// even if that involves reconnecting to the server.
	Errors chan error
	// Logger is a logger that, when set, will be used for logging debug messages
	Logger     *log.Logger
	Cancelfunc context.CancelFunc
	ctx        context.Context
}

type SubscriptionError struct {
	Code    int
	Message string
}

func (e SubscriptionError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

// Subscribe to the Events emitted from the specified url.
// If lastEventId is non-empty it will be sent to the server in case it can replay missed events.
func Subscribe(ctx context.Context, url, lastEventId string) (*Stream, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	return SubscribeWithRequest(ctx, lastEventId, req)
}

// SubscribeWithRequest will take an http.Request to setup the stream, allowing custom headers
// to be specified, authentication to be configured, etc.
func SubscribeWithRequest(ctx context.Context, lastEventId string, req *http.Request) (*Stream, error) {
	stream := &Stream{
		req:         req,
		lastEventId: lastEventId,
		retry:       (time.Millisecond * 3000),
		closed:      false,
		Events:      make(chan Event),
		Errors:      make(chan error),
	}
	stream.c.CheckRedirect = checkRedirect
	stream.ctx = ctx

	cancel, r, err := stream.connect()
	if err != nil {
		return nil, err
	}
	stream.Cancelfunc = cancel
	go stream.stream(r)

	return stream, nil
}

func (stream *Stream) Close() {
	stream.closed = true
}

// Go's http package doesn't copy headers across when it encounters
// redirects so we need to do that manually.
func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	for k, vv := range via[0].Header {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	return nil
}

func (stream *Stream) connect() (context.CancelFunc, io.ReadCloser, error) {
	var resp *http.Response
	var err error
	ctx, cancel := context.WithCancel(stream.ctx)

	stream.req.Header.Set("Cache-Control", "no-cache")
	stream.req.Header.Set("Accept", "text/event-stream")
	if len(stream.lastEventId) > 0 {
		stream.req.Header.Set("Last-Event-ID", stream.lastEventId)
	}
	req := stream.req.WithContext(ctx)
	if resp, err = stream.c.Do(req); err != nil {
		cancel()
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		message, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		cancel()
		err = SubscriptionError{
			Code:    resp.StatusCode,
			Message: string(message),
		}
		return nil, nil, err
	}
	r := resp.Body
	return cancel, r, nil
}

func (stream *Stream) readLoop(r io.ReadCloser) {
	dec := NewDecoder(r)
	for {
		select {
		case <-stream.ctx.Done():
			return
		default:
			if stream.closed {
				stream.Cancelfunc()
				return
			}

			ev, err := dec.Decode()
			if err != nil {
				select {
				case stream.Errors <- err:
					// respond to all errors by reconnecting and trying again
					return
				default:
					// if we can't send to the channel we still need to close
					return
				}
			}

			pub := ev.(*publication)
			if pub.Retry() > 0 {
				stream.retry = time.Duration(pub.Retry()) * time.Millisecond
			}
			if len(pub.Id()) > 0 {
				stream.lastEventId = pub.Id()
			}
			stream.Events <- ev
		}
	}
}

func (stream *Stream) stream(r io.ReadCloser) {
	defer r.Close()
	stream.readLoop(r)
	backoff := stream.retry
	for {
		stream.Cancelfunc()
		time.Sleep(backoff)
		if stream.Logger != nil {
			stream.Logger.Printf("Reconnecting in %0.4f secs\n", backoff.Seconds())
		}

		// NOTE: because of the defer we're opening the new connection
		// before closing the old one. Shouldn't be a problem in practice,
		// but something to be aware of.
		cancel, next, err := stream.connect()
		stream.Cancelfunc = cancel
		if err != nil {
			stream.Errors <- err
			backoff *= 2
			continue
		}
		go stream.stream(next)
		return
	}
}
