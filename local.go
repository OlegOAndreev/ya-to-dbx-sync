package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/codeglyph/go-dotignore/v2"
	"github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/files"
)

// ignoreRule is a compiled .gitignore-style pattern.
type ignoreRule struct {
	re      *regexp.Regexp
	negate  bool
	dirOnly bool
}

// ignoreSet holds the rules of a single .gitignore file for a directory. dir is a relative path.
type ignoreSet struct {
	dir   string
	rules []ignoreRule
}

type localFile struct {
	File
	LocalPath string
}

func toDropboxLocal(ctx context.Context, dbx files.ContextClient, dirs, skip []string, toPath string, parallel, uploadMaxAttempts int) error {
	toPath = strings.Trim(toPath, "/")

	roots, err := cleanDirs(dirs)
	if err != nil {
		return err
	}

	fmt.Printf("Listing files on local disk...\n")
	var localFiles []localFile
	if err := walkLocal(roots, skip, func(f localFile) {
		localFiles = append(localFiles, f)
	}); err != nil {
		return err
	}
	fmt.Printf("Listed %d files locally\n", len(localFiles))

	fmt.Printf("Listing files on Dropbox...\n")
	var dbxDirs []string
	for _, root := range roots {
		dbxDirs = append(dbxDirs, path.Join(toPath, filepath.Base(root)))
	}
	dbxModTimes := make(map[string]time.Time)
	if err := DropboxList(ctx, dbx, dbxDirs, func(f File) {
		dbxModTimes[strings.ToLower(f.Path)] = f.ModTime
	}); err != nil {
		return err
	}

	remoteFiles := make([]File, len(localFiles))
	localPaths := make(map[string]string, len(localFiles))
	for i, f := range localFiles {
		remoteFiles[i] = f.File
		localPaths[f.Path] = f.LocalPath
	}

	return syncFiles(ctx, remoteFiles, parallel, toPath, dbxModTimes, func(ctx context.Context, f File) error {
		return DropboxUploadFile(ctx, dbx, func() (io.ReadCloser, error) {
			return os.Open(localPaths[f.Path])
		}, f, toPath, uploadMaxAttempts)
	})
}

// Convert all directories into absolute paths.
func cleanDirs(dirs []string) ([]string, error) {
	if len(dirs) == 0 {
		return nil, errors.New("at least on directory must be specified")
	}

	baseSet := make(map[string]string)
	var result []string
	for _, dir := range dirs {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}

		base := filepath.Base(abs)
		if strings.ContainsRune(base, filepath.Separator) {
			return nil, fmt.Errorf("can't sync root directory %q", dir)
		}

		if prev, ok := baseSet[base]; ok {
			return nil, fmt.Errorf("directories %q and %q would both be uploaded to the same directory", prev, dir)
		}
		baseSet[base] = dir
		result = append(result, abs)
	}
	return result, nil
}

func walkLocal(roots []string, skip []string, fn func(localFile)) error {
	skipMatcher, err := dotignore.NewPatternMatcher(skip)
	if err != nil {
		return err
	}
	for _, root := range roots {
		if err := walkLocalRoot(root, skipMatcher, fn); err != nil {
			return err
		}
	}
	return nil
}

func walkLocalRoot(root string, skipMatcher *dotignore.PatternMatcher, fn func(localFile)) error {
	matcher, err := dotignore.NewRepositoryMatcher(root)
	if err != nil {
		return err
	}

	remoteBase := filepath.Base(root)
	return matcher.Walk(func(path string, d fs.DirEntry, ignored bool, err error) error {
		if ignored {
			return nil
		}
		shouldSkip, err := skipMatcher.Matches(path)
		if err != nil {
			return err
		}
		if shouldSkip {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			fn(localFile{
				File: File{
					Path:    filepath.ToSlash(filepath.Join(remoteBase, relPath)),
					Size:    info.Size(),
					ModTime: info.ModTime(),
				},
				LocalPath: path,
			})
		}
		return nil
	})
}
