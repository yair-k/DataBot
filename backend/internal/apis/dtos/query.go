package dtos

type ExecuteQueryRequest struct {
	MessageID string `json:"message_id" binding:"required"`
	QueryID   string `json:"query_id" binding:"required"`
	StreamID  string `json:"stream_id" binding:"required"`
}

type RollbackQueryRequest struct {
	MessageID string `json:"message_id" binding:"required"`
	QueryID   string `json:"query_id" binding:"required"`
	StreamID  string `json:"stream_id" binding:"required"`
}

type CancelQueryExecutionRequest struct {
	MessageID string `json:"message_id" binding:"required"`
	QueryID   string `json:"query_id" binding:"required"`
	StreamID  string `json:"stream_id" binding:"required"`
}

type QueryExecutionResponse struct {
	ChatID            string          `json:"chat_id"`
	MessageID         string          `json:"message_id"`
	QueryID           string          `json:"query_id"`
	IsExecuted        bool            `json:"is_executed"`
	IsRolledBack      bool            `json:"is_rolled_back"`
	ExecutionTime     *int            `json:"execution_time"`
	ExecutionResult   interface{}     `json:"execution_result"`
	Error             *QueryError     `json:"error,omitempty"`
	TotalRecordsCount *int            `json:"total_records_count"`
	ActionButtons     *[]ActionButton `json:"action_buttons,omitempty"`
	ActionAt          *string         `json:"action_at,omitempty"`
}

type QueryResultsRequest struct {
	MessageID string `json:"message_id" binding:"required"`
	QueryID   string `json:"query_id" binding:"required"`
	StreamID  string `json:"stream_id" binding:"required"`
	Offset    int    `json:"offset" binding:"required"`
}

type QueryResultsResponse struct {
	ChatID            string          `json:"chat_id"`
	MessageID         string          `json:"message_id"`
	QueryID           string          `json:"query_id"`
	ExecutionResult   interface{}     `json:"execution_result"`
	Error             *QueryError     `json:"error,omitempty"`
	TotalRecordsCount *int            `json:"total_records_count"`
	ActionButtons     *[]ActionButton `json:"action_buttons,omitempty"`
	ActionAt          *string         `json:"action_at,omitempty"`
}

type EditQueryRequest struct {
	MessageID string `json:"message_id" binding:"required"`
	QueryID   string `json:"query_id" binding:"required"`
	Query     string `json:"query" binding:"required"`
}

type EditQueryResponse struct {
	ChatID    string `json:"chat_id"`
	MessageID string `json:"message_id"`
	QueryID   string `json:"query_id"`
	Query     string `json:"query"`
	IsEdited  bool   `json:"is_edited"`
}
