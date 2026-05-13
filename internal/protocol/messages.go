package protocol

type Request struct {
	Action string `json:"action"`
	Topic string `json:"topic,omitempty"`
	Data string `json:"data,omitempty"`
}

type Response struct {
	Type string `json:"type"`
	Status string `json:"status,omitempty"`
	Topic string `json:"topic,omitempty"`
	Data string `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}