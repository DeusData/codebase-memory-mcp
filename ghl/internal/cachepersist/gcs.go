package cachepersist

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const gcsOperationTimeout = 10 * time.Minute

// NewGCS creates a syncer that persists SQLite artifacts directly to GCS.
func NewGCS(ctx context.Context, runtimeDir, bucket, prefix string) (*Syncer, error) {
	runtimeDir = strings.TrimSpace(runtimeDir)
	bucket = strings.TrimSpace(bucket)
	if runtimeDir == "" {
		return nil, fmt.Errorf("cachepersist: runtime dir is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("cachepersist: gcs bucket is required")
	}
	if err := os.MkdirAll(runtimeDir, 0o750); err != nil {
		return nil, fmt.Errorf("cachepersist: create runtime dir: %w", err)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("cachepersist: create gcs client: %w", err)
	}

	prefix = normalizeGCSPrefix(prefix)
	artifactDir := "gs://" + bucket
	if prefix != "" {
		artifactDir += "/" + prefix
	}

	return &Syncer{
		RuntimeDir:  runtimeDir,
		ArtifactDir: artifactDir,
		backend: &gcsBackend{
			client: client,
			bucket: bucket,
			prefix: prefix,
		},
	}, nil
}

type gcsBackend struct {
	client *storage.Client
	bucket string
	prefix string
}

func (b *gcsBackend) Hydrate(runtimeDir string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), gcsOperationTimeout)
	defer cancel()

	files, err := b.listDBObjects(ctx)
	if err != nil {
		return 0, err
	}

	copied := 0
	for _, attrs := range files {
		name := path.Base(attrs.Name)
		reader, err := b.client.Bucket(b.bucket).Object(attrs.Name).NewReader(ctx)
		if err != nil {
			return copied, fmt.Errorf("cachepersist: open gcs object %s: %w", attrs.Name, err)
		}
		err = copyReaderAtomic(reader, filepath.Join(runtimeDir, name), 0o640)
		_ = reader.Close()
		if err != nil {
			return copied, fmt.Errorf("cachepersist: hydrate %s: %w", name, err)
		}
		copied++
	}
	return copied, nil
}

func (b *gcsBackend) PersistProject(runtimeDir, project string) (int, error) {
	project = strings.TrimSpace(project)
	if project == "" {
		return 0, fmt.Errorf("cachepersist: project is required")
	}

	pattern := filepath.Join(runtimeDir, project+".db*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, fmt.Errorf("cachepersist: glob project artifacts: %w", err)
	}
	sort.Strings(matches)

	copied := 0
	for _, src := range matches {
		info, err := os.Stat(src)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return copied, fmt.Errorf("cachepersist: stat %s: %w", src, err)
		}
		if info.IsDir() || !isDBArtifact(info.Name()) {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), gcsOperationTimeout)
		if err := b.uploadFile(ctx, src, info.Name()); err != nil {
			cancel()
			return copied, fmt.Errorf("cachepersist: persist %s: %w", info.Name(), err)
		}
		cancel()
		copied++
	}
	return copied, nil
}

func (b *gcsBackend) CountArtifacts() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), gcsOperationTimeout)
	defer cancel()

	files, err := b.listDBObjects(ctx)
	if err != nil {
		return 0, err
	}
	return len(files), nil
}

func (b *gcsBackend) Close() error {
	return b.client.Close()
}

func (b *gcsBackend) uploadFile(ctx context.Context, srcPath, name string) error {
	input, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer input.Close()

	writer := b.client.Bucket(b.bucket).Object(b.objectName(name)).NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	if _, err := io.Copy(writer, input); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

func (b *gcsBackend) listDBObjects(ctx context.Context) ([]*storage.ObjectAttrs, error) {
	query := &storage.Query{Prefix: b.listPrefix()}
	iter := b.client.Bucket(b.bucket).Objects(ctx, query)

	files := make([]*storage.ObjectAttrs, 0)
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("cachepersist: list gcs objects: %w", err)
		}
		if attrs == nil || strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		if !isDBArtifact(path.Base(attrs.Name)) {
			continue
		}
		files = append(files, attrs)
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name < files[j].Name
	})
	return files, nil
}

func (b *gcsBackend) listPrefix() string {
	if b.prefix == "" {
		return ""
	}
	return b.prefix + "/"
}

func (b *gcsBackend) objectName(name string) string {
	if b.prefix == "" {
		return name
	}
	return b.prefix + "/" + name
}

func normalizeGCSPrefix(prefix string) string {
	return strings.Trim(strings.TrimSpace(prefix), "/")
}
