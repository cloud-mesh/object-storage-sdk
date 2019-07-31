package object_storage_sdk

import (
	"errors"
	"fmt"
	"net/http"
)

func CheckResponse(resp *http.Response) error {
	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusMultipleChoices {
		return errors.New(fmt.Sprintf("code=%d", resp.StatusCode))
	}

	return nil
}