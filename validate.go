package main

import (
	"bytes"
	"image/jpeg"
	"path"
	"strings"
)

func isJpegPath(p string) bool {
	ext := path.Ext(p)
	return strings.EqualFold(ext, ".jpg") || strings.EqualFold(ext, ".jpeg")
}

func IsValidJpeg(contents []byte) bool {
	img, err := jpeg.Decode(bytes.NewReader(contents))
	if err != nil {
		return false
	}
	size := img.Bounds().Max
	return size.X > 0 && size.Y > 0
}
