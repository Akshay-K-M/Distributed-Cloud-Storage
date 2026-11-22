// This file defines stable API error codes and their HTTP status mapping.
package api

// Error codes shared by API clients and servers.
const (
	CodeInvalidRequest     = "INVALID_REQUEST"
	CodeChecksumMismatch   = "CHECKSUM_MISMATCH"
	CodeObjectTooLarge     = "OBJECT_TOO_LARGE"
	CodeUnauthorized       = "UNAUTHORIZED"
	CodeObjectNotFound     = "OBJECT_NOT_FOUND"
	CodeChunkNotFound      = "CHUNK_NOT_FOUND"
	CodeUnknownWorker      = "UNKNOWN_WORKER"
	CodeNameConflict       = "NAME_CONFLICT"
	CodeInternal           = "INTERNAL"
	CodeStorageUnavailable = "STORAGE_UNAVAILABLE"
	CodeOverloaded         = "OVERLOADED"
)

// ErrorBody is the JSON error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail is an error code, message and request ID.
type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// StatusFor maps an API error code to an HTTP status.
func StatusFor(code string) int {
	switch code {
	case CodeInvalidRequest, CodeChecksumMismatch, CodeObjectTooLarge:
		return 400
	case CodeUnauthorized:
		return 401
	case CodeObjectNotFound, CodeChunkNotFound, CodeUnknownWorker:
		return 404
	case CodeNameConflict:
		return 409
	case CodeStorageUnavailable, CodeOverloaded:
		return 503
	default:
		return 500
	}
}
