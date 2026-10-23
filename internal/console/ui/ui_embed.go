//go:build embedui

package ui

import (
	"embed"
	"io/fs"
)

// The web app's production build, which the console image and make
// console-binary copy here from web/dist before building with -tags embedui.
//
//go:embed all:dist
var dist embed.FS

// FS returns the built web app.
func FS() fs.FS {
	files, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err)
	}
	return files
}
