package object_storage_sdk

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func CheckResponse(resp *http.Response) error {
	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusMultipleChoices {
		return errors.New(fmt.Sprintf("code=%d", resp.StatusCode))
	}

	return nil
}

func HeaderToObjectMeta(header http.Header) (objectMeta ObjectMeta, err error) {
	size, err := strconv.Atoi(header.Get("Content-Length"))
	if err != nil {
		return
	}
	lastModified, err := time.Parse(time.RFC1123, header.Get("Last-Modified"))

	return ObjectMeta{
		ContentType:   header.Get("Content-Type"),
		ContentLength: size,
		ETag:          header.Get("Etag"),
		LastModified:  lastModified,
	}, nil
}
