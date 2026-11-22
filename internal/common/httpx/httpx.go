// This file provides shared HTTP clients, middleware, and JSON responses.
package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"objstore/internal/api"
)

type ctxKey int

const requestIDKey ctxKey = 1

// NewHTTPClient is the shared process HTTP client.
func NewHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	return &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         dialer.DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
			// Downloads stage the whole object before sending headers. Worker RPCs
			// set their own deadlines on the request context.
			ForceAttemptHTTP2: true,
		},
	}
}

// NewServer wraps a handler with header and idle timeouts for streaming requests.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// RequestIDFrom returns the request id on ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}

// WithRequestID stores a request id on ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// WriteJSON writes v as JSON with status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes a structured API error.
func WriteError(w http.ResponseWriter, r *http.Request, code, message string) {
	id := ""
	if r != nil {
		id = RequestIDFrom(r.Context())
		if id == "" {
			id = r.Header.Get("X-Request-Id")
		}
	}
	WriteJSON(w, api.StatusFor(code), api.ErrorBody{
		Error: api.ErrorDetail{Code: code, Message: message, RequestID: id},
	})
}

// DecodeJSON reads a JSON object from r into v.
func DecodeJSON(r io.Reader, v any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// Chain applies middleware outermost-first.
func Chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// Recover is the outermost middleware.
func Recover(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if rec == http.ErrAbortHandler {
						panic(rec)
					}
					log.Error("panic", "err", rec, "path", r.URL.Path)
					WriteError(w, r, api.CodeInternal, "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RequestID sets X-Request-Id, generating one if missing.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.Header.Get("X-Request-Id"))
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
	})
}

func newRequestID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "r-00000000"
	}
	return "r-" + hex.EncodeToString(b[:])
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += n
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// AccessLog logs method, path, status, duration, bytes.
func AccessLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: 0}
			next.ServeHTTP(sw, r)
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			log.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"duration_ms", time.Since(start).Milliseconds(),
				"bytes", sw.bytes,
				"request_id", RequestIDFrom(r.Context()),
			)
		})
	}
}

// BearerAuth rejects unauthenticated /internal/* requests.
func BearerAuth(token string) func(http.Handler) http.Handler {
	want := "Bearer " + token
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/internal/") {
				got := r.Header.Get("Authorization")
				if token == "" || got != want {
					WriteError(w, r, api.CodeUnauthorized, "invalid or missing bearer token")
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
