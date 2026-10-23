//go:build !embedui

package ui

import (
	"io/fs"
	"testing/fstest"
)

// FS returns no files: this console was built without the web app, as Go
// builds are by default, so that they need no JavaScript toolchain. Run the
// web app with make web-dev instead, or build with -tags embedui.
func FS() fs.FS { return fstest.MapFS{} }
