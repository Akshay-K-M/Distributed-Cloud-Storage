// This file implements the CLI's HTTP client and streaming file transfers.
// commands.go handles user options and presentation; this file handles the wire
// protocol: URLs, request bodies, response decoding, and streaming file bytes.
// All requests go to the controller. The CLI never chooses workers or chunks.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"objstore/internal/api"
	"objstore/internal/common/httpx"
)

// APIClient shares the controller address and HTTP connection machinery across
// operations. ctx on each operation can cancel its HTTP request.
type APIClient struct {
	endpoint string
	http     *http.Client
}

// newAPIClient prepares a client for the chosen controller. A supplied HTTP
// client allows custom settings or test behavior; otherwise use shared defaults.
// Removing a trailing / keeps endpoint + /api/... from producing double slashes.
func newAPIClient(endpoint string, httpClient *http.Client) *APIClient {
	if httpClient == nil {
		httpClient = httpx.NewHTTPClient()
	}
	return &APIClient{
		endpoint: strings.TrimRight(endpoint, "/"),
		http:     httpClient,
	}
}

// ParseTarget classifies a command argument without making any network request.
// Examples: obj:photos/cat.jpg -> remote name "photos/cat.jpg";
// id:abc -> remote ID "abc"; local.txt -> local path "local.txt".
// This helper only recognizes markers; it does not check whether an object exists.
func ParseTarget(arg string) (isRemote bool, isID bool, value string) {
	if strings.HasPrefix(arg, "id:") {
		return true, true, strings.TrimPrefix(arg, "id:")
	}
	if strings.HasPrefix(arg, "obj:") {
		val := strings.TrimPrefix(arg, "obj:")
		val = strings.TrimPrefix(val, "/") // Strip leading slash
		return true, false, val
	}
	return false, false, arg
}

// ResolveObjectID converts a CLI target into an ID usable by the HTTP API.
// An obj:name target needs an exact-name lookup; id:abc already supplies the ID
// and skips that lookup. Bare names are rejected here, which is why cat/stat
// require a prefix even though ls accepts an unmarked search prefix.
// Returns the ID and resolved name, or an error; direct-ID targets return no name.
func (c *APIClient) ResolveObjectID(ctx context.Context, target string) (string, string, error) {
	isRemote, isID, val := ParseTarget(target)
	if !isRemote {
		return "", "", fmt.Errorf("INVALID_USAGE: target %q is not a remote path (must start with obj: or id:)", target)
	}
	if isID {
		return val, "", nil
	}

	// Query GET /api/v1/objects?name=<exact>
	u := fmt.Sprintf("%s/api/v1/objects?name=%s", c.endpoint, url.QueryEscape(val))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", "", fmt.Errorf("INTERNAL: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("UNAVAILABLE: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", parseAPIError(resp)
	}

	var list api.ListResponse
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return "", "", fmt.Errorf("INTERNAL: failed to parse response: %w", err)
	}

	if len(list.Objects) == 0 {
		return "", "", fmt.Errorf("OBJECT_NOT_FOUND: no such object: %s", val)
	}

	return list.Objects[0].ObjectID, list.Objects[0].Name, nil
}

// Upload sends one local file and its metadata to POST /api/v1/objects, then
// returns the committed object's metadata. Used by: ocs cp a.txt obj:notes/a.txt.
// The multipart format packages a name, custom labels, and file bytes together.
// A pipe feeds the HTTP request as the file is read, avoiding a whole-file buffer
// or temporary multipart file. Chunking and replication happen in the controller.
func (c *APIClient) Upload(
	ctx context.Context,
	localPath, remoteName string,
	overwrite bool,
	meta map[string]string,
	contentType string,
	quiet bool,
	errOut io.Writer,
) (*api.Object, error) {
	// Part 1: open the local source and find its size for progress reporting.
	file, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("INVALID_REQUEST: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("INVALID_REQUEST: %w", err)
	}

	// Part 2: connect a multipart producer to the outgoing HTTP request. The
	// writer and reader run concurrently; the pipe keeps them moving together.
	pr, pw := io.Pipe()
	mpw := multipart.NewWriter(pw)

	go func() {
		var writeErr error
		defer func() {
			if writeErr != nil {
				_ = pw.CloseWithError(writeErr)
			} else {
				_ = pw.Close()
			}
		}()

		// Send identifying fields before file bytes so the controller can prepare
		// the upload before processing the stream.
		if writeErr = mpw.WriteField("name", remoteName); writeErr != nil {
			return
		}

		// Write metadata part second
		if len(meta) > 0 {
			metaBytes, mErr := json.Marshal(meta)
			if mErr != nil {
				writeErr = mErr
				return
			}
			if writeErr = mpw.WriteField("metadata", string(metaBytes)); writeErr != nil {
				return
			}
		}

		// Write file part
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filepath.Base(localPath)))
		if contentType != "" {
			h.Set("Content-Type", contentType)
		}
		partWriter, pErr := mpw.CreatePart(h)
		if pErr != nil {
			writeErr = pErr
			return
		}

		var reader io.Reader = file
		if !quiet && errOut != nil && fileInfo.Size() > 0 {
			reader = newProgressReader(file, fileInfo.Size(), errOut)
		}

		if _, writeErr = io.Copy(partWriter, reader); writeErr != nil {
			return
		}

		writeErr = mpw.Close()
	}()

	// Part 3: send the streaming body. --overwrite becomes a query option;
	// the Content-Type here describes the multipart envelope, not the file type.
	u := fmt.Sprintf("%s/api/v1/objects", c.endpoint)
	if overwrite {
		u += "?overwrite=true"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, pr)
	if err != nil {
		_ = pr.Close()
		return nil, fmt.Errorf("INTERNAL: %w", err)
	}
	req.Header.Set("Content-Type", mpw.FormDataContentType())

	resp, err := c.http.Do(req)
	if err != nil {
		_ = pr.Close()
		return nil, fmt.Errorf("UNAVAILABLE: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return nil, parseAPIError(resp)
	}

	// Part 4: require the controller's commit confirmation and decode the result.
	// Finishing the local progress bar alone does not mean the object committed.
	var created api.Object
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, fmt.Errorf("INTERNAL: failed to parse created object: %w", err)
	}

	return &created, nil
}

// Download requests one object's bytes from the controller by ID.
// With a destination, it streams bytes there and closes the response body:
// cp passes a file, while cat passes stdout. With dst == nil, it returns an
// open response body so verify can hash it; that caller must close the body.
// The controller handles chunk ordering and replica fallback, not this function.
func (c *APIClient) Download(ctx context.Context, objectID string, dst io.Writer) (*http.Response, error) {
	u := fmt.Sprintf("%s/api/v1/objects/%s", c.endpoint, objectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("INTERNAL: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("UNAVAILABLE: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, parseAPIError(resp)
	}

	if dst != nil {
		defer resp.Body.Close()
		if _, err := io.Copy(dst, resp.Body); err != nil {
			return nil, fmt.Errorf("INTERNAL: download stream interrupted: %w", err)
		}
	}

	return resp, nil
}

// GetMetadata fetches object properties, custom labels, and chunk/replica details
// from GET /objects/{id}/metadata. Used by stat and meta get; no file bytes move.
func (c *APIClient) GetMetadata(ctx context.Context, objectID string) (*api.ObjectDetail, error) {
	u := fmt.Sprintf("%s/api/v1/objects/%s/metadata", c.endpoint, objectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("INTERNAL: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("UNAVAILABLE: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp)
	}

	var detail api.ObjectDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		return nil, fmt.Errorf("INTERNAL: failed to parse metadata: %w", err)
	}

	return &detail, nil
}

// PatchMetadata sends only the custom labels to change and returns updated detail.
// Example JSON: {"owner":"alice","album":null} sets owner and removes album;
// labels not mentioned stay unchanged. Both meta set and meta rm use this API.
func (c *APIClient) PatchMetadata(ctx context.Context, objectID string, patch map[string]*string) (*api.ObjectDetail, error) {
	b, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("INVALID_REQUEST: %w", err)
	}

	u := fmt.Sprintf("%s/api/v1/objects/%s/metadata", c.endpoint, objectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u, strings.NewReader(string(b)))
	if err != nil {
		return nil, fmt.Errorf("INTERNAL: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("UNAVAILABLE: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp)
	}

	var updated api.ObjectDetail
	if err := json.NewDecoder(resp.Body).Decode(&updated); err != nil {
		return nil, fmt.Errorf("INTERNAL: failed to parse metadata: %w", err)
	}

	return &updated, nil
}

// Delete asks the controller to delete one object using HTTP DELETE.
// A successful response acknowledges logical deletion; worker-file cleanup is
// background work on the server. Recursive rm calls this once per listed object.
func (c *APIClient) Delete(ctx context.Context, objectID string) error {
	u := fmt.Sprintf("%s/api/v1/objects/%s", c.endpoint, objectID)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return fmt.Errorf("INTERNAL: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("UNAVAILABLE: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return parseAPIError(resp)
	}

	return nil
}

// List fetches all pages of matching objects and grouped name prefixes.
// Example: prefix="photos/", delimiter="/" gives folder-like grouping;
// delimiter="" gives a flat listing. Pagination is hidden from command handlers.
// The server returns bounded pages, but this client accumulates all results in
// memory before returning, so very large listings still consume client memory.
func (c *APIClient) List(ctx context.Context, prefix, delimiter string) ([]api.ListObject, []string, error) {
	var allObjects []api.ListObject
	var allPrefixes []string
	cursor := ""

	for {
		// Request one page, passing the cursor from the previous page if present.
		params := url.Values{}
		if prefix != "" {
			params.Set("prefix", prefix)
		}
		if delimiter != "" {
			params.Set("delimiter", delimiter)
		}
		if cursor != "" {
			params.Set("cursor", cursor)
		}

		u := fmt.Sprintf("%s/api/v1/objects?%s", c.endpoint, params.Encode())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("INTERNAL: %w", err)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, nil, fmt.Errorf("UNAVAILABLE: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			defer resp.Body.Close()
			return nil, nil, parseAPIError(resp)
		}

		var page api.ListResponse
		decErr := json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if decErr != nil {
			return nil, nil, fmt.Errorf("INTERNAL: failed to parse list response: %w", decErr)
		}

		// Collect this page and continue only if the server supplies another cursor.
		allObjects = append(allObjects, page.Objects...)
		allPrefixes = append(allPrefixes, page.CommonPrefixes...)

		if !page.Truncated || page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	return allObjects, allPrefixes, nil
}

// AdminStatus fetches the controller's cluster overview for ocs status:
// worker liveness, configured replication settings, and object/chunk health counts.
func (c *APIClient) AdminStatus(ctx context.Context) (*api.AdminStatus, error) {
	u := fmt.Sprintf("%s/api/v1/admin/status", c.endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("INTERNAL: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("UNAVAILABLE: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, parseAPIError(resp)
	}

	var status api.AdminStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("INTERNAL: failed to parse admin status: %w", err)
	}

	return &status, nil
}

// parseAPIError turns a failed HTTP response into an error the CLI can display.
// Prefer the server's structured code/message, e.g. OBJECT_NOT_FOUND: object not
// found. If the body is unrecognized, fall back to its HTTP status number.
func parseAPIError(resp *http.Response) error {
	var body api.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err == nil && body.Error.Code != "" {
		return fmt.Errorf("%s: %s", body.Error.Code, body.Error.Message)
	}
	return fmt.Errorf("HTTP_%d: unexpected response status %d", resp.StatusCode, resp.StatusCode)
}

// progressReader wraps a source stream to measure bytes read and elapsed time.
// This measures feeding the upload, not individual chunks or durable replicas.
type progressReader struct {
	r         io.Reader
	total     int64
	read      int64
	start     time.Time
	lastPrint time.Time
	out       io.Writer
}

// newProgressReader prepares progress tracking around a file reader. Upload uses
// it when progress is enabled, passing file size and stderr as the output stream.
func newProgressReader(r io.Reader, total int64, out io.Writer) *progressReader {
	now := time.Now()
	return &progressReader{
		r:         r,
		total:     total,
		start:     now,
		lastPrint: now,
		out:       out,
	}
}

// Read forwards a read to the source and updates the upload progress display.
// Example: 3145728 / 3145728 bytes (... MB/s). Intermediate output is throttled
// to about every 200 ms. Completion can print twice: once on reaching total bytes
// and again on a later end-of-file read. These lines do not count chunk uploads.
func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	pr.read += int64(n)

	now := time.Now()
	if errors.Is(err, io.EOF) || pr.read == pr.total {
		elapsed := now.Sub(pr.start).Seconds()
		if elapsed <= 0 {
			elapsed = 0.001
		}
		mbps := (float64(pr.read) / (1024 * 1024)) / elapsed
		fmt.Fprintf(pr.out, "\r%d / %d bytes (%.2f MB/s)\n", pr.read, pr.total, mbps)
	} else if now.Sub(pr.lastPrint) >= 200*time.Millisecond {
		elapsed := now.Sub(pr.start).Seconds()
		if elapsed <= 0 {
			elapsed = 0.001
		}
		mbps := (float64(pr.read) / (1024 * 1024)) / elapsed
		fmt.Fprintf(pr.out, "\r%d / %d bytes (%.2f MB/s)", pr.read, pr.total, mbps)
		pr.lastPrint = now
	}

	return n, err
}

// formatBytes converts a byte count into a short display string for status.
// Example: 1048576 becomes "1.0 MiB". This changes presentation, not stored data.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
