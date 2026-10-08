package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// GDriveClient lists files on Google Drive and uploads to it over the Drive v3 API.
type GDriveClient struct {
	svc *drive.Service

	// foldersMu guards folders, the cache of Drive folder paths to their IDs.
	foldersMu sync.Mutex
	folders   map[string]string
}

const (
	gdriveFolderMIME = "application/vnd.google-apps.folder"
	gdrivePageSize   = 1000
	// gdriveChunkSize is the resumable upload chunk size; Drive requires a multiple of 256 KiB.
	gdriveChunkSize = 8 << 20
)

func gdriveOAuthConfig(clientID, clientSecret string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       []string{drive.DriveScope},
		Endpoint:     google.Endpoint,
	}
}

func parseClientFile(clientFile string) (string, string, error) {
	f, err := os.Open(clientFile)
	if err != nil {
		return "", "", err
	}
	defer f.Close()
	var r struct {
		Installed struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		} `json:"installed"`
	}
	if err := json.NewDecoder(f).Decode(&r); err != nil {
		return "", "", err
	}
	return r.Installed.ClientID, r.Installed.ClientSecret, nil
}

func GDriveAuthorize(ctx context.Context, clientFile, tokenFile string) error {
	clientID, clientSecret, err := parseClientFile(clientFile)
	if err != nil {
		return err
	}
	if _, err := gdriveAuthorize(ctx, gdriveOAuthConfig(clientID, clientSecret), tokenFile); err != nil {
		return err
	}
	fmt.Printf("Google Drive token stored in %s\n", tokenFile)
	return nil
}

func NewGDriveClient(ctx context.Context, clientFile, tokenFile string) (*GDriveClient, error) {
	clientID, clientSecret, err := parseClientFile(clientFile)
	if err != nil {
		return nil, err
	}
	cfg := gdriveOAuthConfig(clientID, clientSecret)

	token, err := gdriveToken(ctx, cfg, tokenFile)
	if err != nil {
		return nil, err
	}
	svc, err := drive.NewService(ctx, option.WithTokenSource(cfg.TokenSource(ctx, token)))
	if err != nil {
		return nil, err
	}
	return &GDriveClient{svc: svc, folders: make(map[string]string)}, nil
}

func gdriveToken(ctx context.Context, cfg *oauth2.Config, tokenFile string) (*oauth2.Token, error) {
	b, err := os.ReadFile(tokenFile)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if strings.TrimSpace(string(b)) == "" {
		return gdriveAuthorize(ctx, cfg, tokenFile)
	}
	token := new(oauth2.Token)
	if err := json.Unmarshal(b, token); err != nil {
		return nil, fmt.Errorf("%s: %w", tokenFile, err)
	}
	return token, nil
}

func (c *GDriveClient) List(ctx context.Context, dirs []string, fn func(File)) error {
	dirPrefixes := make([]string, len(dirs))
	for i, dir := range dirs {
		if dir = strings.Trim(dir, "/"); dir != "" {
			dir += "/"
		}
		dirPrefixes[i] = dir
	}

	var walk func(folderID, dir string) error
	walk = func(folderID, dir string) error {
		for pageToken := ""; ; {
			page, err := c.listPage(ctx, folderID, pageToken)
			if err != nil {
				return err
			}
			for _, f := range page.Files {
				child := path.Join(dir, f.Name)
				if f.MimeType == gdriveFolderMIME {
					// Recurse only into the requested directories and their parent directories leading to them.
					if !hasOneOfPrefixes(child, dirPrefixes) && !isPrefixOfOne(child, dirPrefixes) {
						continue
					}
					if err := walk(f.Id, child); err != nil {
						return err
					}
					continue
				}
				if !hasOneOfPrefixes(child, dirPrefixes) {
					continue
				}
				modTime, _ := time.Parse(time.RFC3339, f.ModifiedTime)
				fn(File{Path: child, Size: f.Size, ModTime: modTime})
			}
			if page.NextPageToken == "" {
				return nil
			}
			pageToken = page.NextPageToken
		}
	}
	return walk("root", "")
}

func isPrefixOfOne(parent string, dirs []string) bool {
	parent += "/"
	for _, dir := range dirs {
		if strings.HasPrefix(dir, parent) {
			return true
		}
	}
	return false
}

func (c *GDriveClient) listPage(ctx context.Context, folderID, pageToken string) (*drive.FileList, error) {
	var page *drive.FileList
	err := gdriveDo(ctx, "drive folder "+folderID, func() error {
		call := c.svc.Files.List().
			Q("'" + folderID + "' in parents and trashed = false").
			Fields("nextPageToken, files(id, name, mimeType, size, modifiedTime)").
			PageSize(gdrivePageSize)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		var err error
		page, err = call.Context(ctx).Do()
		return err
	})
	if err != nil {
		return nil, err
	}
	return page, nil
}

func gdriveRetryable(err error) bool {
	if apiErr, ok := errors.AsType[*googleapi.Error](err); ok {
		return retryableStatus(apiErr.Code)
	}
	return true
}

func gdriveDo(ctx context.Context, what string, fn func() error) error {
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if attempt >= maxAttempts || !gdriveRetryable(err) {
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

func (c *GDriveClient) UploadFile(ctx context.Context, reader func() (io.ReadCloser, error), f File, toPath string) error {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := c.uploadOnce(ctx, reader, f, toPath)
		if err == nil {
			return nil
		} else if attempt >= maxAttempts {
			return err
		}
		fmt.Printf("UPLOAD RETRY %d/%d %s: %v\n", attempt, maxAttempts, f.Path, err)
		delay := time.Duration(1+rand.IntN(10)) * time.Second
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (c *GDriveClient) uploadOnce(ctx context.Context, reader func() (io.ReadCloser, error), f File, toPath string) error {
	folderID, err := c.folderID(ctx, path.Join(toPath, path.Dir(f.Path)))
	if err != nil {
		return err
	}
	id, err := c.findFile(ctx, folderID, path.Base(f.Path))
	if err != nil {
		return err
	}

	body, err := reader()
	if err != nil {
		return err
	}
	defer body.Close()

	meta := &drive.File{
		Name:         path.Base(f.Path),
		ModifiedTime: f.ModTime.Format(time.RFC3339), // keeps the disk's timestamp, so later runs can compare ages
	}
	// The SDK streams the body in resumable chunks and retries failed chunks from its in-memory buffer.
	option := googleapi.ChunkSize(gdriveChunkSize)
	if id == "" {
		meta.Parents = []string{folderID}
		_, err = c.svc.Files.Create(meta).Fields("id").Media(body, option).Context(ctx).Do()
	} else {
		_, err = c.svc.Files.Update(id, meta).Fields("id").Media(body, option).Context(ctx).Do()
	}
	return err
}

func (c *GDriveClient) folderID(ctx context.Context, dir string) (string, error) {
	c.foldersMu.Lock()
	defer c.foldersMu.Unlock()
	dir = strings.Trim(dir, "/")
	if dir == "." {
		dir = ""
	}
	return c.folderIDLocked(ctx, dir)
}

func (c *GDriveClient) folderIDLocked(ctx context.Context, dir string) (string, error) {
	if dir == "" {
		return "root", nil
	}
	if id, ok := c.folders[dir]; ok {
		return id, nil
	}

	parentPath, name := path.Split(dir)
	parentID, err := c.folderIDLocked(ctx, strings.Trim(parentPath, "/"))
	if err != nil {
		return "", err
	}

	id, err := c.findFolder(ctx, parentID, name)
	if err != nil {
		return "", err
	}
	if id == "" {
		if err := gdriveDo(ctx, fmt.Sprintf("create Drive folder %q", dir), func() error {
			f, err := c.svc.Files.Create(&drive.File{
				Name:     name,
				MimeType: gdriveFolderMIME,
				Parents:  []string{parentID},
			}).Fields("id").Context(ctx).Do()
			if err != nil {
				return err
			}
			id = f.Id
			return nil
		}); err != nil {
			return "", err
		}
	}

	c.folders[dir] = id
	return id, nil
}

func (c *GDriveClient) findFolder(ctx context.Context, parentID, name string) (string, error) {
	return c.findChild(ctx, parentID, name, "mimeType = '"+gdriveFolderMIME+"'")
}

func (c *GDriveClient) findFile(ctx context.Context, parentID, name string) (string, error) {
	return c.findChild(ctx, parentID, name, "mimeType != '"+gdriveFolderMIME+"'")
}

func (c *GDriveClient) findChild(ctx context.Context, parentID, name, mimeFilter string) (string, error) {
	var id string
	err := gdriveDo(ctx, fmt.Sprintf("find Drive item %q", name), func() error {
		page, err := c.svc.Files.List().
			Q(fmt.Sprintf("'%s' in parents and name = '%s' and %s and trashed = false", parentID, gdriveEscapeName(name), mimeFilter)).
			Fields("files(id)").
			PageSize(1).
			Context(ctx).Do()
		if err != nil {
			return err
		}
		id = ""
		if len(page.Files) > 0 {
			id = page.Files[0].Id
		}
		return nil
	})
	return id, err
}

// gdriveEscapeName escapes a name for use inside a Drive query string.
func gdriveEscapeName(name string) string {
	name = strings.ReplaceAll(name, `\`, `\\`)
	return strings.ReplaceAll(name, "'", `\'`)
}

// gdriveAuthorize runs the OAuth loopback flow: Google no longer supports the copy/paste OOB redirect, so the code
// arrives on a temporary loopback server.
func gdriveAuthorize(ctx context.Context, cfg *oauth2.Config, tokenFile string) (*oauth2.Token, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	cfg.RedirectURL = "http://" + listener.Addr().String()

	codes := make(chan string, 1)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			if e := q.Get("error"); e != "" {
				http.Error(w, e, http.StatusForbidden)
				fmt.Fprintf(os.Stderr, "Google Drive authorization failed: %s\n", e)
				codes <- ""
				return
			}
			code := q.Get("code")
			if code == "" {
				http.Error(w, "no authorization code in request", http.StatusBadRequest)
				return
			}
			fmt.Fprintln(w, "Authorized, you can close this tab <3")
			codes <- code
		}),
	}
	go func() { _ = srv.Serve(listener) }()
	defer srv.Close()

	url := cfg.AuthCodeURL("", oauth2.AccessTypeOffline, oauth2.ApprovalForce)
	fmt.Printf("Open this URL in a browser and click Allow: %s\n", url)

	var code string
	select {
	case code = <-codes:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if code == "" {
		return nil, fmt.Errorf("authorization failed")
	}

	token, err := cfg.Exchange(ctx, code)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(token)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(tokenFile, append(b, '\n'), 0o600); err != nil {
		return nil, err
	}

	return token, nil
}
