package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/filetransfer"
	"golang.org/x/sync/errgroup"
)

const (
	defaultYaClientId    = "75e7915c8b34495599b6bea072347f86"
	defaultDropboxAppKey = "aca15zhg3xn7dkg"

	// maxAttempts is how many times a failed request or transfer is tried before giving up.
	maxAttempts = 10
)

type File struct {
	Path    string
	Size    int64
	ModTime time.Time
}

func readList(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

func uploadFile(ctx context.Context, c *YaDiskClient, up *filetransfer.Uploader, f File) error {
	tmp, err := c.Download(ctx, f.Path)
	if err != nil {
		return err
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name())

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := uploadOnce(ctx, up, tmp, f)
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

func uploadOnce(ctx context.Context, up *filetransfer.Uploader, tmp *os.File, f File) error {
	source, err := filetransfer.FileUpload(tmp.Name())
	if err != nil {
		return err
	}
	arg := files.NewCommitInfo("/" + f.Path)
	arg.Mode = &files.WriteMode{Tagged: dropbox.Tagged{Tag: files.WriteModeOverwrite}}
	clientModified := dropbox.DBXTime(f.ModTime) // keeps the disk's timestamp, so later runs can compare ages
	arg.ClientModified = &clientModified

	_, err = up.Upload(ctx, source, arg, filetransfer.UploadOptions{MaxAttempts: 1})
	return err
}

func toDropbox(ctx context.Context, ya *YaDiskClient, dirs []string, appKey, tokenFile string, parallel int) error {
	dbx, err := dropboxClient(ctx, appKey, tokenFile)
	if err != nil {
		return err
	}

	var fs []File
	for _, dir := range dirs {
		err := ya.List(ctx, dir, parallel, func(f File) {
			fs = append(fs, f)
		})
		if err != nil {
			return err
		}
	}

	dbxModTimes, err := dropboxListModTimes(ctx, dbx, dirs)
	if err != nil {
		return err
	}

	var toSync []File
	var bytesToSync int64
	var newer int
	seen := make(map[string]bool, len(fs))
	for _, f := range fs {
		if seen[f.Path] { // overlapping -dirs entries list a file twice
			continue
		}

		seen[f.Path] = true
		if modTime, ok := dbxModTimes[strings.ToLower(f.Path)]; ok {
			// Allow some slack if file times are rounded by dropbox when stored.
			if modTime.After(f.ModTime.Add(-time.Second)) {
				fmt.Printf("SKIPPED %s (already on Dropbox)\n", f.Path)
				newer++
				continue
			}
		}
		toSync = append(toSync, f)
		bytesToSync += f.Size
	}
	fmt.Printf("Already %d on Dropbox, %d to upload\n", newer, len(toSync))
	if len(toSync) == 0 {
		return nil // nothing to upload
	}

	up := filetransfer.NewUploader(dbx)

	startTime := time.Now()
	var totalSynced atomic.Int32
	var totalSize atomic.Int64
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(parallel)
	for _, f := range toSync {
		eg.Go(func() error {
			startFileTime := time.Now()
			err := uploadFile(egCtx, ya, up, f)
			if err != nil {
				return err
			}

			curSynced := totalSynced.Add(1)
			totalSize.Add(f.Size)
			mb := float64(f.Size) / (1 << 20)
			fileTime := time.Since(startFileTime)
			speed := mb / fileTime.Seconds()
			totalMb := float64(totalSize.Load()) / (1 << 20)
			totalTime := time.Since(startTime)
			avgSpeed := totalMb / totalTime.Seconds()
			remainingMb := float64(bytesToSync)/(1<<20) - totalMb
			eta := time.Duration(remainingMb / avgSpeed * float64(time.Second))
			fmt.Printf("SYNCED [%d/%d] ETA %s\t%s (%.1f Mb, %.1f Mb/s, total %v, avg %.1f Mb/s)\n",
				curSynced, len(toSync), eta.Round(time.Second), f.Path, mb, speed, totalTime.Round(time.Second), avgSpeed)

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	totalMb := float64(totalSize.Load()) / (1 << 20)
	total := time.Since(startTime)
	avgSpeed := totalMb / total.Seconds()
	fmt.Printf("SUCCESS %d synced, %d already on Dropbox (%.2f Mb in %s, %.2f Mb/s average)\n", len(toSync), newer, totalMb, total.Round(time.Millisecond), avgSpeed)
	return nil
}

func main() {
	action := flag.String("action", "list-all", "what to do: \"list-all\" lists every file, \"list-top\" lists top-level directories, \"to-dropbox\" uploads the directories listed in -dirs to Dropbox, skipping files that are newer there")
	dir := flag.String("path", "", "[for list-all and list-top] subdirectory to list; default is the whole disk")
	dirsStr := flag.String("dirs", "", "[for to-dropbox] comma-separate list of Yandex Disk directories to upload, relative to the disk root")
	dirsFile := flag.String("dirs-file", "", "[for to-dropbox] file with the Yandex Disk directories to upload, one per line, relative to the disk root")
	parallel := flag.Int("parallel", 10, "number of files to upload concurrently (used by to-dropbox)")
	appKey := flag.String("dropbox-app-key", defaultDropboxAppKey, "Dropbox app key")
	tokenFile := flag.String("token", "dropbox.token", "file caching the Dropbox OAuth token; a missing or empty file asks the user to authorize")
	yaClientID := flag.String("ya-client-id", defaultYaClientId, "Yandex Disk app client ID")
	yaTokenFile := flag.String("ya-oauth-token", "ya.token", "file caching the Yandex Disk OAuth token; a missing or empty file asks the user to authorize")
	flag.Parse()

	if *parallel == 0 {
		*parallel = 1
	}

	var dirs []string
	if *dirsStr != "" {
		dirs = strings.Split(*dirsStr, ",")
		for i := range dirs {
			dirs[i] = strings.TrimSpace(dirs[i])
		}
	} else if *dirsFile != "" {
		var err error
		dirs, err = readList(*dirsFile)
		if err != nil {
			log.Fatal(err)
		}
		if len(dirs) == 0 {
			log.Fatalf("No directories listed in %s", *dirsFile)
		}
	} else {
		dirs = []string{""}
	}

	ctx := context.Background()
	c, err := NewYaDiskClient(*yaClientID, *yaTokenFile)
	if err != nil {
		log.Fatal(err)
	}
	switch *action {
	case "list-all":
		n := 0
		if err := c.List(ctx, *dir, *parallel, func(f File) {
			fmt.Printf("%12d  %s  %s\n", f.Size, f.ModTime.Format(time.RFC3339), f.Path)
			n++
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println(n, "files")
	case "list-top":
		err := c.ListDirs(ctx, *dir, 1, func(p string) { fmt.Println(p) })
		if err != nil {
			log.Fatal(err)
		}
	case "to-dropbox":
		err := toDropbox(ctx, c, dirs, *appKey, *tokenFile, *parallel)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown action %q, want list-all, list-top or to-dropbox", *action)
	}
}
