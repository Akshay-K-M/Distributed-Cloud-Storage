package workerclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
)

var (
	ErrChunkNotFound     = errors.New("chunk not found")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrUnavailable       = errors.New("unavailable")
)

// WorkerRef identifies a worker for controller→worker calls.
type WorkerRef struct {
	ID   string
	Addr string
}

// Client is the kickoff contract used by the controller.
type Client interface {
	PutChunk(ctx context.Context, w WorkerRef, chunkID, sha256hex string, size int64, body io.Reader) error
	GetChunk(ctx context.Context, w WorkerRef, chunkID string) (body io.ReadCloser, size int64, storedSHA string, err error)
	DeleteChunk(ctx context.Context, w WorkerRef, chunkID string) error
	ListChunks(ctx context.Context, w WorkerRef) (*api.Inventory, error)
	Scrub(ctx context.Context, w WorkerRef) error
}

const chunkOpTimeout = 30 * time.Second

// HTTPClient talks to a worker over REST.
type HTTPClient struct {
	http  *http.Client
	token string
}

// New returns a real worker HTTP client.
func New(c *http.Client, token string) *HTTPClient {
	if c == nil {
		c = httpx.NewHTTPClient()
	}
	return &HTTPClient{http: c, token: token}
}

func (c *HTTPClient) PutChunk(ctx context.Context, w WorkerRef, chunkID, sha256hex string, size int64, body io.Reader) error {
	ctx, cancel := context.WithTimeout(ctx, chunkOpTimeout)
	defer cancel()
	url := w.Addr + "/internal/chunks/" + chunkID
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	req.Header.Set("X-Chunk-SHA256", sha256hex)
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer drain(resp)
	return mapStatus(resp)
}

func (c *HTTPClient) GetChunk(ctx context.Context, w WorkerRef, chunkID string) (io.ReadCloser, int64, string, error) {
	ctx, cancel := context.WithTimeout(ctx, chunkOpTimeout)
	url := w.Addr + "/internal/chunks/" + chunkID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		return nil, 0, "", fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, 0, "", fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if err := mapStatus(resp); err != nil {
		drain(resp)
		cancel()
		return nil, 0, "", err
	}
	size := resp.ContentLength
	if size < 0 {
		size, _ = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	}
	sha := resp.Header.Get("X-Chunk-SHA256")
	return &cancelCloser{ReadCloser: resp.Body, cancel: cancel}, size, sha, nil
}

func (c *HTTPClient) DeleteChunk(ctx context.Context, w WorkerRef, chunkID string) error {
	ctx, cancel := context.WithTimeout(ctx, chunkOpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.Addr+"/internal/chunks/"+chunkID, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	return mapStatus(resp)
}

func (c *HTTPClient) ListChunks(ctx context.Context, w WorkerRef) (*api.Inventory, error) {
	ctx, cancel := context.WithTimeout(ctx, chunkOpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.Addr+"/internal/workers/"+w.ID+"/chunks", nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer drain(resp)
	if err := mapStatus(resp); err != nil {
		return nil, err
	}
	var inv api.Inventory
	if err := json.NewDecoder(resp.Body).Decode(&inv); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return &inv, nil
}

func (c *HTTPClient) Scrub(ctx context.Context, w WorkerRef) error {
	ctx, cancel := context.WithTimeout(ctx, chunkOpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.Addr+"/internal/scrub", nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	c.auth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusAccepted {
		return nil
	}
	return mapStatus(resp)
}

func (c *HTTPClient) auth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.token)
}

func mapStatus(resp *http.Response) error {
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		return nil
	case http.StatusNoContent, http.StatusAccepted:
		return nil
	case http.StatusNotFound:
		return ErrChunkNotFound
	case http.StatusBadRequest:
		var body api.ErrorBody
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if body.Error.Code == api.CodeChecksumMismatch {
			return ErrChecksumMismatch
		}
		return fmt.Errorf("%w: %s", ErrUnavailable, body.Error.Code)
	default:
		if resp.StatusCode >= 500 {
			return ErrUnavailable
		}
		return fmt.Errorf("%w: status %d", ErrUnavailable, resp.StatusCode)
	}
}

func drain(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
	}
}

type cancelCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
