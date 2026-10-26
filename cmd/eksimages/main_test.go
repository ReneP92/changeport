package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const kafka = "123456789012.dkr.ecr.us-east-1.amazonaws.com/changeport/kafka@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// repository is the root of the repository, from this package's directory.
var repository, _ = filepath.Abs(filepath.Join("..", ".."))

// inRepository runs the test in a copy of the two files the command writes.
func inRepository(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	for _, file := range []string{imagesFile, componentFile} {
		data, err := os.ReadFile(filepath.Join(repository, file))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(file)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, file), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)
}

func TestRunSetsAnImage(t *testing.T) {
	inRepository(t)
	if err := run([]string{"kafka=" + kafka}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(imagesFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"kafka": "`+kafka+`"`) || !strings.Contains(string(data), `"agent": ""`) {
		t.Errorf("eks-images.json =\n%s\nwant the Kafka image set and the others kept", data)
	}
}

func TestRunRefusesBadImages(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"kafka"},
		{"broker=" + kafka},
		{"agent=" + kafka},
		{"kafka=changeport/kafka:local"},
	} {
		inRepository(t)
		before, _ := os.ReadFile(imagesFile)
		if err := run(args); err == nil {
			t.Errorf("run(%q) succeeded, want an error", args)
		}
		after, _ := os.ReadFile(imagesFile)
		if string(before) != string(after) {
			t.Errorf("run(%q) changed eks-images.json despite the error", args)
		}
	}
}
