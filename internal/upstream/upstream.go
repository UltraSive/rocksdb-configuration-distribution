package upstream

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

type Client struct {
	URL    string
	Client *http.Client
}

func New(url string, timeout time.Duration) *Client {
	return &Client{
		URL: url,
		Client: &http.Client{
			Timeout: timeout,
		},
	}
}

type Request struct {
	Type string   `json:"type"`
	Keys []string `json:"keys,omitempty"`
}

type Response struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data,omitempty"`
	Error string                `json:"error,omitempty"`
}

func (c *Client) Fetch(key string) ([]byte, bool, error) {
	if c == nil || c.URL == "" {
		return nil, false, nil
	}
	req := Request{Type: "GET", Keys: []string{key}}
	b, _ := json.Marshal(&req)
	httpReq, _ := http.NewRequest("POST", c.URL, bytes.NewReader(b))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 404 {
			return nil, false, nil
		}
		return nil, false, nil
	}
	var r Response
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, false, err
	}
	if r.Type == "ERR" {
		return nil, false, nil
	}
	val, ok := r.Data[key]
	if !ok || val == nil {
		return nil, false, nil
	}
	raw, _ := json.Marshal(val)
	return raw, true, nil
}
