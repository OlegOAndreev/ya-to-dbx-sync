package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// YaDiskClient lists and downloads files on Yandex Disk over its REST API.
type YaDiskClient struct {
	baseURL string
	token   string
	hc      *http.Client
}

const (
	// If true, list all disk and filter out the required files, if false -- do a recursive listing. Listing all
	// elements in Yandex.Disk is almost always much faster than doing a subtree walk.
	useListAll = true

	yaAPIURL     = "https://cloud-api.yandex.net/v1/"
	yaAuthURL    = "https://oauth.yandex.ru/authorize?response_type=token&client_id="
	yaFields     = "_embedded.items.name,_embedded.items.type,_embedded.items.size,_embedded.items.modified"
	yaFlatFields = "items.name,items.path,items.type,items.size,items.modified"
	yaPageLimit  = 1000
)

func NewYaDiskClient(clientID, tokenFile string) (*YaDiskClient, error) {
	token, err := yaToken(clientID, tokenFile)
	if err != nil {
		return nil, err
	}
	cl := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 10 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
		},
	}
	return &YaDiskClient{
		baseURL: yaAPIURL,
		token:   token,
		hc:      cl,
	}, nil
}

func yaToken(clientID, tokenFile string) (string, error) {
	b, err := os.ReadFile(tokenFile)
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if token := strings.TrimSpace(string(b)); token != "" {
		return token, nil
	}

	fmt.Printf("Open this URL in a browser and click Allow: %s%s\n", yaAuthURL, clientID)
	fmt.Print("Then paste the OAuth token here: ")
	token, err := bufio.NewReader(os.Stdin).ReadString('\n')
	token = strings.TrimSpace(token)
	if err != nil && token == "" {
		return "", err
	}
	if token == "" {
		return "", fmt.Errorf("no OAuth token given")
	}
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		return "", err
	}
	return token, nil
}

func retryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (c *YaDiskClient) getJSON(ctx context.Context, u, what string, dst any) error {
	for attempt := 1; ; attempt++ {
		retryable, err := c.getJSONOnce(ctx, u, dst)
		if err == nil {
			return nil
		}
		if !retryable || attempt >= maxAttempts {
			return fmt.Errorf("%s: %w", what, err)
		}
		fmt.Printf("RETRY %d/%d %s: %v\n", attempt, maxAttempts, what, err)
		d := time.Duration(1+rand.IntN(10)) * time.Second
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
}

func (c *YaDiskClient) getJSONOnce(ctx context.Context, u string, dst any) (again bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "OAuth "+c.token)
	resp, err := c.hc.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		status := fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
		return retryableStatus(resp.StatusCode), status
	}
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		return true, err
	}
	return false, nil
}

type yaItem struct {
	Name     string    `json:"name"`
	Type     string    `json:"type"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"modified"`
	Path     string    `json:"path"` // only filled by the flat files endpoint
}

func (c *YaDiskClient) List(ctx context.Context, dirs []string, parallel int, fn func(File)) error {
	if parallel == 0 {
		parallel = 1
	}

	if useListAll {
		// The flat files endpoint lists the whole disk in one sequence, so no directory recursion is needed. It is much
		// faster than directory recursion.
		return c.listAllFiles(ctx, dirs, parallel, fn)
	}

	eg, egCtx := errgroup.WithContext(ctx)
	sema := semaphore.NewWeighted(int64(parallel))
	var fnMu sync.Mutex

	var processDir func(d string) error
	processDir = func(d string) error {
		if err := sema.Acquire(egCtx, 1); err != nil {
			return err
		}
		defer sema.Release(1)

		fmt.Printf("Listing %s\n", d)
		return c.listDir(egCtx, d, func(it yaItem) {
			child := path.Join(d, it.Name)
			if it.Type == "dir" {
				eg.Go(func() error {
					return processDir(child)
				})
			} else {
				fnMu.Lock()
				fn(File{Path: child, Size: it.Size, ModTime: it.Modified})
				fnMu.Unlock()
			}
		})
	}

	for _, dir := range dirs {
		eg.Go(func() error {
			return processDir(strings.Trim(dir, "/"))
		})
	}

	return eg.Wait()
}

func (c *YaDiskClient) listAllFiles(ctx context.Context, dirs []string, parallel int, fn func(File)) error {
	var dirPrefixes []string
	for _, dir := range dirs {
		dir := strings.Trim(dir, "/")
		if dir != "" {
			dirPrefixes = append(dirPrefixes, dir+"/")
		} else {
			dirPrefixes = append(dirPrefixes, "")
		}
	}
	// We list all files in parallel per page.
	eg, egCtx := errgroup.WithContext(ctx)
	var lastStartedOffset atomic.Int32
	var fnMu sync.Mutex
	for range parallel {
		eg.Go(func() error {
			// Try reading the next available page until it becomes empty.
			for {
				curOffset := lastStartedOffset.Add(yaPageLimit) - yaPageLimit
				items, err := c.listAllPage(egCtx, int(curOffset), yaPageLimit)
				if err != nil {
					return err
				}
				if len(items) == 0 {
					return nil
				}
				for _, it := range items {
					if it.Type != "file" {
						continue
					}
					path := strings.TrimPrefix(it.Path, "disk:/")

					if hasOneOfPrefixes(path, dirPrefixes) {
						fnMu.Lock()
						fn(File{Path: path, Size: it.Size, ModTime: it.Modified})
						fnMu.Unlock()
					}
				}
			}
		})
	}

	return eg.Wait()
}

func hasOneOfPrefixes(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (c *YaDiskClient) listDir(ctx context.Context, dir string, fn func(yaItem)) error {
	offset := 0
	for {
		items, err := c.listPage(ctx, dir, offset)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		for _, it := range items {
			fn(it)
		}
		offset += len(items)
	}
}

// Download returns the file contents as a stream; the caller must close it.
// Retries live in the caller, which restarts the whole upload anyway.
func (c *YaDiskClient) Download(ctx context.Context, file string) (io.ReadCloser, error) {
	href, err := c.downloadHref(ctx, file)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, href, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "OAuth "+c.token)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("disk:/%s: %w", file, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("disk:/%s: %s: %s", file, resp.Status, strings.TrimSpace(string(b)))
	}
	return resp.Body, nil
}

func (c *YaDiskClient) downloadHref(ctx context.Context, file string) (string, error) {
	q := url.Values{"path": {"disk:/" + file}}
	var r struct {
		Href string `json:"href"`
	}
	if err := c.getJSON(ctx, c.baseURL+"disk/resources/download?"+q.Encode(), "disk:/"+file, &r); err != nil {
		return "", err
	}
	if r.Href == "" {
		return "", fmt.Errorf("disk:/%s: empty download href", file)
	}
	return r.Href, nil
}

func (c *YaDiskClient) listAllPage(ctx context.Context, offset, pageSize int) ([]yaItem, error) {
	endOffset := offset + pageSize
	var result []yaItem
	// Additional check: try to return the whole page even if the API did not return it all at once.
	for offset < endOffset {
		q := url.Values{
			"fields": {yaFlatFields},
			"limit":  {strconv.Itoa(endOffset - offset)},
			"offset": {strconv.Itoa(offset)},
		}
		var r struct {
			Items []yaItem `json:"items"`
		}
		if err := c.getJSON(ctx, c.baseURL+"disk/resources/files?"+q.Encode(), fmt.Sprintf("disk:/files offset %d", offset), &r); err != nil {
			return nil, err
		}
		if len(r.Items) == 0 {
			break
		}
		result = append(result, r.Items...)
		offset += len(r.Items)
	}
	return result, nil
}

func (c *YaDiskClient) listPage(ctx context.Context, dir string, offset int) ([]yaItem, error) {
	q := url.Values{
		"path":   {"disk:/" + dir},
		"fields": {yaFields},
		"limit":  {strconv.Itoa(yaPageLimit)},
		"offset": {strconv.Itoa(offset)},
	}
	var r struct {
		Embedded struct {
			Items []yaItem `json:"items"`
		} `json:"_embedded"`
	}
	if err := c.getJSON(ctx, c.baseURL+"disk/resources?"+q.Encode(), "disk:/"+dir, &r); err != nil {
		return nil, err
	}
	return r.Embedded.Items, nil
}
