package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
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

func toDropbox(ctx context.Context, ya *YaDiskClient, dbx files.ContextClient, dirs []string, toPath string, parallel int) error {
	toPath = strings.Trim(toPath, "/")

	var fs []File
	if err := ya.List(ctx, dirs, parallel, func(f File) {
		fs = append(fs, f)
	}); err != nil {
		return err
	}
	log.Printf("Listed %d files on Yandex.Disk\n", len(fs))

	// Check the same Dropbox subfolder the files are uploaded to, or the mod-time comparison below would never see them.
	dbxDirs := make([]string, len(dirs))
	for i, d := range dirs {
		dbxDirs[i] = path.Join(toPath, d)
	}
	dbxModTimes := make(map[string]time.Time)
	if err := DropboxList(ctx, dbx, dbxDirs, func(f File) {
		dbxModTimes[strings.ToLower(f.Path)] = f.ModTime
	}); err != nil {
		return err
	}

	return syncFiles(ctx, fs, parallel, toPath, dbxModTimes, func(ctx context.Context, f File) error {
		return DropboxUploadFile(ctx, dbx, func() (io.ReadCloser, error) {
			return ya.Download(ctx, f.Path)
		}, f, toPath)
	})
}

func toDrive(ctx context.Context, ya *YaDiskClient, drv *GDriveClient, dirs []string, toPath string, parallel int) error {
	toPath = strings.Trim(toPath, "/")

	var fs []File
	if err := ya.List(ctx, dirs, parallel, func(f File) {
		fs = append(fs, f)
	}); err != nil {
		return err
	}
	log.Printf("Listed %d files on Yandex.Disk\n", len(fs))

	// Check the same Drive folders the files are uploaded to, or the mod-time comparison below would never see them.
	driveDirs := make([]string, len(dirs))
	for i, d := range dirs {
		driveDirs[i] = path.Join(toPath, d)
	}
	driveModTimes := make(map[string]time.Time)
	if err := drv.List(ctx, driveDirs, func(f File) {
		driveModTimes[strings.ToLower(f.Path)] = f.ModTime
	}); err != nil {
		return err
	}

	return syncFiles(ctx, fs, parallel, toPath, driveModTimes, func(ctx context.Context, f File) error {
		return drv.UploadFile(ctx, func() (io.ReadCloser, error) {
			return ya.Download(ctx, f.Path)
		}, f, toPath)
	})
}

func syncFiles(ctx context.Context, fs []File, parallel int, toPath string, dstModTimes map[string]time.Time, upload func(ctx context.Context, f File) error) error {
	var toSync []File
	var bytesToSync int64
	var newer int
	seen := make(map[string]bool, len(fs))
	for _, f := range fs {
		if seen[f.Path] { // overlapping -dirs entries list a file twice
			continue
		}

		seen[f.Path] = true
		// dstModTimes are expected to have lowercased keys
		dstPath := strings.ToLower(path.Join(toPath, f.Path))
		if modTime, ok := dstModTimes[dstPath]; ok {
			// Allow some slack if file times are rounded by the destination when stored.
			if modTime.After(f.ModTime.Add(-time.Second)) {
				fmt.Printf("SKIPPED %s (already in destination)\n", f.Path)
				newer++
				continue
			}
		}
		toSync = append(toSync, f)
		bytesToSync += f.Size
	}
	fmt.Printf("%d already in destination, %d to upload\n", newer, len(toSync))
	if len(toSync) == 0 {
		return nil // nothing to upload
	}

	startTime := time.Now()
	var totalSynced atomic.Int32
	var totalSize atomic.Int64
	var totalErrors atomic.Int64
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(parallel)
	for _, f := range toSync {
		eg.Go(func() error {
			startFileTime := time.Now()
			err := upload(egCtx, f)
			if err != nil {
				fmt.Printf("FAILED %s: %v\n", f.Path, err)
				totalErrors.Add(1)
				return nil
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
			fmt.Printf("SYNCED [%d/%d] ETA %15s\t%s (%.1f Mb, %.1f Mb/s, total %v, avg %.1f Mb/s)\n",
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
	fmt.Printf("SUCCESS %d synced, %d already on %s (%.2f Mb in %s, %.2f Mb/s average)\n", totalSynced.Load(), newer, dstName, totalMb, total.Round(time.Millisecond), avgSpeed)
	if n := totalErrors.Load(); n > 0 {
		fmt.Printf("ERRORS %d. Try syncing again!\n", n)
	}
	return nil
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	action := flag.String("action", "list-all", "what to do: \"list-all\" lists every file, \"to-dropbox\" and \"to-gdrive\" upload the directories listed in -dirs to Dropbox or Google Drive respectively, skipping files that are newer there, \"gdrive-auth\" only runs the Google Drive authorization flow and stores the token")
	dirsStr := flag.String("dirs", "", "[for to-dropbox, to-drive and gdrive-list] comma-separated list of directories to process, relative to the remote root")
	dirsFile := flag.String("dirs-file", "", "[for to-dropbox, to-drive and gdrive-list] file with the directories to process, one per line, relative to the remote root")
	toPath := flag.String("to-path", "", "[for to-dropbox and to-drive] Dropbox/Google Drive subfolder to upload into")
	parallel := flag.Int("parallel", 10, "number of files to upload concurrently (used by to-dropbox and to-drive)")
	appKey := flag.String("dropbox-app-key", defaultDropboxAppKey, "Dropbox app key")
	dbxTokenFile := flag.String("dropbox-token", "dropbox.token", "file caching the Dropbox OAuth token; a missing or empty file asks the user to authorize")
	yaClientID := flag.String("ya-client-id", defaultYaClientId, "Yandex Disk app client ID")
	yaTokenFile := flag.String("ya-oauth-token", "ya.token", "file caching the Yandex Disk OAuth token; a missing or empty file asks the user to authorize")
	gdriveClientFile := flag.String("gdrive-client-file", "gdrive-client.json", "[for gdrive-auth, gdrive-list and to-drive] Google Drive OAuth client credentials file (the file that is saved from Auth Platform Clients page)")
	gdriveTokenFile := flag.String("gdrive-token", "gdrive.token", "[for gdrive-auth, gdrive-list and to-drive] file to store the Google Drive OAuth token in")
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

	switch *action {
	case "list-all":
		c, err := NewYaDiskClient(*yaClientID, *yaTokenFile)
		if err != nil {
			log.Fatal(err)
		}

		startTime := time.Now()
		n := 0
		if err := c.List(ctx, dirs, *parallel, func(f File) {
			fmt.Printf("%12d  %s  %s\n", f.Size, f.ModTime.Format(time.RFC3339), f.Path)
			n++
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d files in %v\n", n, time.Since(startTime).Round(time.Second))
	case "to-dropbox":
		c, err := NewYaDiskClient(*yaClientID, *yaTokenFile)
		if err != nil {
			log.Fatal(err)
		}
		dbx, err := NewDropboxClient(ctx, *appKey, *dbxTokenFile)
		if err != nil {
			log.Fatal(err)
		}

		if err := toDropbox(ctx, c, dbx, dirs, *toPath, *parallel); err != nil {
			log.Fatal(err)
		}
	case "to-gdrive":
		c, err := NewYaDiskClient(*yaClientID, *yaTokenFile)
		if err != nil {
			log.Fatal(err)
		}
		drv, err := NewGDriveClient(ctx, *gdriveClientFile, *gdriveTokenFile)
		if err != nil {
			log.Fatal(err)
		}

		if err := toDrive(ctx, c, drv, dirs, *toPath, *parallel); err != nil {
			log.Fatal(err)
		}
	case "gdrive-auth":
		if err := GDriveAuthorize(ctx, *gdriveClientFile, *gdriveTokenFile); err != nil {
			log.Fatal(err)
		}
		log.Printf("Token stored in %s, you can now copy it to destination machine\n", *gdriveTokenFile)
	case "gdrive-list":
		c, err := NewGDriveClient(ctx, *gdriveClientFile, *gdriveTokenFile)
		if err != nil {
			log.Fatal(err)
		}

		startTime := time.Now()
		n := 0
		if err := c.List(ctx, dirs, func(f File) {
			fmt.Printf("%12d  %s  %s\n", f.Size, f.ModTime.Format(time.RFC3339), f.Path)
			n++
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d files in %v\n", n, time.Since(startTime).Round(time.Second))
	default:
		log.Fatalf("unknown action %q, want list-all, to-dropbox, to-gdrive, gdrive-auth", *action)
	}
}
