// This file implements the CLI's HTTP client and streaming file transfers.
package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

type APIClient struct {
	endpoint string
	http     *http.Client
}

func newAPIClient(endpoint string, httpClient *http.Client) *APIClient {
	if httpClient == nil {
		httpClient = httpx.NewHTTPClient()
	}
	return &APIClient{
		endpoint: strings.TrimRight(endpoint, "/"),
		http:     httpClient,
	}
}

// ParseTarget parses a target path argument like obj:photos/cat.jpg or id:<uuid> or local.txt.
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

func (c *APIClient) Upload(
	ctx context.Context,
	localPath, remoteName string,
	overwrite bool,
	meta map[string]string,
	contentType string,
	quiet bool,
	errOut io.Writer,
) (*api.Object, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("INVALID_REQUEST: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("INVALID_REQUEST: %w", err)
	}

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

		// Write name part first
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

	var created api.Object
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, fmt.Errorf("INTERNAL: failed to parse created object: %w", err)
	}

	return &created, nil
}

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

func (c *APIClient) List(ctx context.Context, prefix, delimiter string) ([]api.ListObject, []string, error) {
	var allObjects []api.ListObject
	var allPrefixes []string
	cursor := ""

	for {
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

		allObjects = append(allObjects, page.Objects...)
		allPrefixes = append(allPrefixes, page.CommonPrefixes...)

		if !page.Truncated || page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}

	return allObjects, allPrefixes, nil
}

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

func parseAPIError(resp *http.Response) error {
	var body api.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err == nil && body.Error.Code != "" {
		return fmt.Errorf("%s: %s", body.Error.Code, body.Error.Message)
	}
	return fmt.Errorf("HTTP_%d: unexpected response status %d", resp.StatusCode, resp.StatusCode)
}

type progressReader struct {
	r         io.Reader
	total     int64
	read      int64
	start     time.Time
	lastPrint time.Time
	out       io.Writer
}

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

// HexEqual compares two sha256 hex strings.
func HexEqual(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

func SHA256Reader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
