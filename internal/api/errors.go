// This file defines stable API error codes and their HTTP status mapping.
package api

// Frozen error codes (plan section 4.3 / 5).
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

// ErrorBody is the frozen JSON error envelope.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail is the frozen error object.
type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// StatusFor maps a frozen error code to an HTTP status.
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
