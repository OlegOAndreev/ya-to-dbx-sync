package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
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
	yaAPIURL    = "https://cloud-api.yandex.net/v1/"
	yaAuthURL   = "https://oauth.yandex.ru/authorize?response_type=token&client_id="
	yaFields    = "_embedded.items.name,_embedded.items.type,_embedded.items.size,_embedded.items.modified"
	yaPageLimit = 1000
)

func NewYaDiskClient(clientID, tokenFile string) (*YaDiskClient, error) {
	token, err := yaToken(clientID, tokenFile)
	if err != nil {
		return nil, err
	}
	return &YaDiskClient{
		baseURL: yaAPIURL,
		token:   token,
		hc:      http.DefaultClient,
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
}

func (c *YaDiskClient) List(ctx context.Context, dir string, parallel int, fn func(File)) error {
	if parallel == 0 {
		parallel = 1
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

	if err := processDir(strings.Trim(dir, "/")); err != nil {
		return err
	}

	return eg.Wait()
}

func (c *YaDiskClient) ListDirs(ctx context.Context, dir string, maxDepth int, fn func(string)) error {
	type item struct {
		path  string
		depth int
	}
	stack := []item{{strings.Trim(dir, "/"), 0}}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		d := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		err := c.listDir(ctx, d.path, func(it yaItem) {
			if it.Type != "dir" {
				return
			}
			child := path.Join(d.path, it.Name)
			fn(child)
			if d.depth+1 < maxDepth { // deeper reads only serve deeper listings
				stack = append(stack, item{child, d.depth + 1})
			}
		})
		if err != nil {
			return err
		}
	}
	return nil
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

func (c *YaDiskClient) Download(ctx context.Context, file string) (_ *os.File, err error) {
	tmp, err := os.CreateTemp("", "*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil { // don't leave a partial download behind
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	if err := c.downloadTo(ctx, file, tmp); err != nil {
		return nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return tmp, nil
}

func (c *YaDiskClient) downloadTo(ctx context.Context, file string, tmp *os.File) error {
	for attempt := 1; ; attempt++ {
		href, err := c.DownloadHref(ctx, file)
		if err != nil {
			if attempt >= maxAttempts {
				return err
			}
			continue
		}

		retryable, err := c.downloadOnce(ctx, href, tmp)
		if err == nil {
			return nil
		}
		if !retryable || attempt >= maxAttempts {
			return fmt.Errorf("disk:/%s: %w", file, err)
		}
		fmt.Printf("DOWNLOAD RETRY %d/%d %s: %v\n", attempt, maxAttempts, file, err)
		delay := time.Duration(1+rand.IntN(10)) * time.Second
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := tmp.Truncate(0); err != nil {
			return err
		}
	}
}

func (c *YaDiskClient) downloadOnce(ctx context.Context, href string, tmp *os.File) (again bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, href, nil)
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
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		return true, err
	}
	return false, nil
}

func (c *YaDiskClient) DownloadHref(ctx context.Context, file string) (string, error) {
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
