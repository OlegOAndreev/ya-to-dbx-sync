package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/oauth"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/retry"
	"golang.org/x/oauth2"
)

func NewDropboxClient(ctx context.Context, appKey, tokenFile string) (files.ContextClient, error) {
	token, err := dropboxLoadToken(tokenFile)
	if err != nil {
		return nil, err
	}
	if token == nil {
		if token, err = dropboxAuthorize(ctx, appKey, tokenFile); err != nil {
			return nil, err
		}
	}
	return files.NewContext(dropbox.Config{
		TokenSource: oauth.TokenSource(ctx, appKey, token),
		RetryPolicy: &retry.Policy{
			MaxRetries: maxAttempts,
		},
	}), nil
}

func dropboxLoadToken(tokenFile string) (*oauth2.Token, error) {
	b, err := os.ReadFile(tokenFile)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(b)) == 0 {
		return nil, nil
	}
	token := new(oauth2.Token)
	if err := json.Unmarshal(b, token); err != nil {
		return nil, fmt.Errorf("%s: %w", tokenFile, err)
	}
	return token, nil
}

func dropboxAuthorize(ctx context.Context, appKey, tokenFile string) (*oauth2.Token, error) {
	flow, err := oauth.NewPKCEFlow(appKey)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Open this URL in a browser and click Allow: %s\n", flow.AuthCodeURL())
	fmt.Print("Then paste the authorization code here: ")
	code, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && strings.TrimSpace(code) == "" {
		return nil, err
	}
	token, err := flow.Exchange(ctx, strings.TrimSpace(code))
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

func DropboxListModTimes(ctx context.Context, dbx files.ContextClient, dirs []string) (map[string]time.Time, error) {
	modTimes := make(map[string]time.Time)
	for _, dir := range dirs {
		arg := files.NewListFolderArg("/" + strings.Trim(dir, "/"))
		arg.Recursive = true
		res, err := dbx.ListFolderContext(ctx, arg)
		if err != nil {
			if strings.Contains(err.Error(), "path/not_found") {
				continue
			}
			return nil, err
		}

		for {
			for _, entry := range res.Entries {
				if f, ok := entry.(*files.FileMetadata); ok {
					modTimes[strings.ToLower(strings.TrimPrefix(f.PathLower, "/"))] = time.Time(f.ClientModified)
				}
			}
			if !res.HasMore {
				break
			}
			if res, err = dbx.ListFolderContinueContext(ctx, files.NewListFolderContinueArg(res.Cursor)); err != nil {
				return nil, err
			}
		}
	}
	return modTimes, nil
}

func DropboxGetModTime(ctx context.Context, dbx files.ContextClient, path string) (time.Time, error) {
	var zeroTime = time.Unix(0, 0)
	res, err := dbx.GetMetadataContext(ctx, files.NewGetMetadataArg("/"+strings.Trim(path, "/")))
	if err != nil {
		if strings.Contains(err.Error(), "path/not_found") {
			return zeroTime, nil
		}
		return zeroTime, err
	}
	if metadata, ok := res.(*files.FileMetadata); ok {
		return time.Time(metadata.ClientModified), nil
	}
	return zeroTime, nil
}
