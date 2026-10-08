package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

// GDriveClient lists and downloads files on Google Drive over the Drive v3 API.
type GDriveClient struct {
	svc *drive.Service
}

const (
	gdriveFolderMIME = "application/vnd.google-apps.folder"
	gdriveNativeMIME = "application/vnd.google-apps."
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

func NewGDriveClient(ctx context.Context, clientID, clientSecret, tokenFile string) (*GDriveClient, error) {
	cfg := gdriveOAuthConfig(clientID, clientSecret)

	token, err := gdriveToken(ctx, cfg, tokenFile)
	if err != nil {
		return nil, err
	}
	svc, err := drive.NewService(ctx, option.WithTokenSource(cfg.TokenSource(ctx, token)))
	if err != nil {
		return nil, err
	}
	return &GDriveClient{svc: svc}, nil
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
