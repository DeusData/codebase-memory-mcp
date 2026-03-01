package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractBinaryFromTarGz(t *testing.T) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	content := []byte("fake binary content")
	hdr := &tar.Header{
		Name:     "codebase-memory-mcp-linux-amd64",
		Mode:     0o700,
		Size:     int64(len(content)),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gw.Close()

	extracted, err := extractBinaryFromTarGz(buf.Bytes())
	if err != nil {
		t.Fatalf("extractBinaryFromTarGz error: %v", err)
	}
	if !bytes.Equal(extracted, content) {
		t.Fatalf("extracted content mismatch: %q vs %q", extracted, content)
	}
}

func TestExtractBinaryFromTarGz_NotFound(t *testing.T) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	hdr := &tar.Header{
		Name:     "some-other-file",
		Mode:     0o600,
		Size:     5,
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gw.Close()

	_, err := extractBinaryFromTarGz(buf.Bytes())
	if err == nil {
		t.Fatal("expected error when binary not found in archive")
	}
}

func TestExtractBinaryFromTarGz_InvalidData(t *testing.T) {
	_, err := extractBinaryFromTarGz([]byte("not a valid tar.gz"))
	if err == nil {
		t.Fatal("expected error for invalid tar.gz data")
	}
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "source")
	dst := filepath.Join(dir, "dest")

	content := []byte("test content for copy")
	if err := os.WriteFile(src, content, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile error: %v", err)
	}

	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Fatalf("copy mismatch: %q vs %q", data, content)
	}
}

func TestCopyFile_SourceNotFound(t *testing.T) {
	dir := t.TempDir()
	err := copyFile(filepath.Join(dir, "nonexistent"), filepath.Join(dir, "dest"))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
}
