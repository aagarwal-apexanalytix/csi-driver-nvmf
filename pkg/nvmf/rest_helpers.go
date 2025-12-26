package nvmf

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"k8s.io/klog/v2"
)

type RestError struct {
	Method     string
	URL        string
	StatusCode int
	Body       []byte
}

func (e *RestError) Error() string {
	trim := strings.TrimSpace(string(e.Body))
	if trim == "" {
		trim = "<empty body>"
	}
	return fmt.Sprintf("REST %s %s error %d: %s", e.Method, e.URL, e.StatusCode, trim)
}

func (e *RestError) BodyString() string { return strings.TrimSpace(string(e.Body)) }

func IsRestStatus(err error, code int) bool {
	var re *RestError
	if errors.As(err, &re) {
		return re.StatusCode == code
	}
	return false
}

func (cs *ControllerServer) restDo(method, url string, body []byte, username, password string) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(username) != "" {
		req.SetBasicAuth(username, password)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := cs.client.Do(req)
	if err != nil {
		klog.Errorf("REST %s %s failed: %v", method, url, err)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 300 {
		klog.Errorf("REST %s %s error %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(respBody)))
		return nil, &RestError{
			Method:     method,
			URL:        url,
			StatusCode: resp.StatusCode,
			Body:       respBody,
		}
	}
	return respBody, nil
}

func (cs *ControllerServer) restGet(path, restURL, username, password string) ([]map[string]interface{}, error) {
	body, err := cs.restDo("GET", strings.TrimRight(restURL, "/")+path, nil, username, password)
	if err != nil {
		return nil, err
	}

	// Try list
	var list []map[string]interface{}
	if err := json.Unmarshal(body, &list); err == nil {
		return list, nil
	}

	// Try single object
	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err == nil && len(obj) > 0 {
		return []map[string]interface{}{obj}, nil
	}

	klog.Errorf("Failed to decode REST GET %s response: %s", path, string(body))
	return nil, fmt.Errorf("failed to decode REST GET %s", path)
}

func (cs *ControllerServer) restPost(path string, data any, restURL, username, password string) error {
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal REST POST body for %s: %w", path, err)
	}
	_, err = cs.restDo("POST", strings.TrimRight(restURL, "/")+path, jsonBody, username, password)
	return err
}

func (cs *ControllerServer) restPatch(path string, data any, restURL, username, password string) error {
	jsonBody, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal REST PATCH body for %s: %w", path, err)
	}
	_, err = cs.restDo("PATCH", strings.TrimRight(restURL, "/")+path, jsonBody, username, password)
	return err
}

func (cs *ControllerServer) restDelete(path, restURL, username, password string) error {
	_, err := cs.restDo("DELETE", strings.TrimRight(restURL, "/")+path, nil, username, password)
	return err
}
