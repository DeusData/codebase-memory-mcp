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
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
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
	if len(files) == 0 {
		return 0, nil
	}

	// Parallel download with up to 32 concurrent workers.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(32)
	var copied atomic.Int64

	for _, attrs := range files {
		attrs := attrs
		g.Go(func() error {
			name := path.Base(attrs.Name)
			dst := filepath.Join(runtimeDir, name)

			// Skip if already exists and same size.
			if info, statErr := os.Stat(dst); statErr == nil && info.Size() == attrs.Size {
				copied.Add(1)
				return nil
			}

			reader, rErr := b.client.Bucket(b.bucket).Object(attrs.Name).NewReader(gctx)
			if rErr != nil {
				return fmt.Errorf("cachepersist: open %s: %w", attrs.Name, rErr)
			}
			wErr := copyReaderAtomic(reader, dst, 0o640)
			_ = reader.Close()
			if wErr != nil {
				return fmt.Errorf("cachepersist: hydrate %s: %w", name, wErr)
			}
			copied.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return int(copied.Load()), err
	}
	return int(copied.Load()), nil
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

func (b *gcsBackend) PersistOrgDB(runtimeDir string) (int, error) {
	// After PRAGMA wal_checkpoint(TRUNCATE), all data is in the main .db file.
	// Upload ONLY the .db file — not WAL/SHM — to ensure atomic consistency.
	// Hydration restores just the .db and deletes any stale WAL files.
	srcDir := filepath.Join(runtimeDir, "org")
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("cachepersist: read org dir: %w", err)
	}
	copied := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".db") {
			continue
		}
		// Skip WAL/SHM journal files — only persist the main .db
		if strings.HasSuffix(name, ".db-wal") || strings.HasSuffix(name, ".db-shm") {
			continue
		}
		src := filepath.Join(srcDir, name)
		objName := "org/" + name
		if b.prefix != "" {
			objName = b.prefix + "/org/" + name
		}
		ctx, cancel := context.WithTimeout(context.Background(), gcsOperationTimeout)
		if err := b.uploadFileToObject(ctx, src, objName); err != nil {
			cancel()
			return copied, fmt.Errorf("cachepersist: persist org %s to gcs: %w", name, err)
		}
		cancel()
		copied++
	}
	return copied, nil
}

func (b *gcsBackend) HydrateOrgDB(runtimeDir string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), gcsOperationTimeout)
	defer cancel()

	prefix := "org/"
	if b.prefix != "" {
		prefix = b.prefix + "/org/"
	}

	dstDir := filepath.Join(runtimeDir, "org")
	if err := os.MkdirAll(dstDir, 0o750); err != nil {
		return 0, fmt.Errorf("cachepersist: create org dir: %w", err)
	}

	// Delete any stale WAL/SHM files before restoring the .db.
	// The persisted .db is self-contained (checkpoint was run before persist).
	for _, suffix := range []string{"-wal", "-shm"} {
		walPath := filepath.Join(dstDir, "org.db"+suffix)
		os.Remove(walPath) // ignore error if file doesn't exist
	}

	// List all org .db objects first.
	query := &storage.Query{Prefix: prefix}
	iter := b.client.Bucket(b.bucket).Objects(ctx, query)

	var objects []*storage.ObjectAttrs
	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("cachepersist: list gcs org objects: %w", err)
		}
		if attrs == nil || strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		name := path.Base(attrs.Name)
		// Only restore .db files — WAL was flushed into .db before persist
		if !strings.HasSuffix(name, ".db") ||
			strings.HasSuffix(name, ".db-wal") ||
			strings.HasSuffix(name, ".db-shm") {
			continue
		}
		objects = append(objects, attrs)
	}

	if len(objects) == 0 {
		return 0, nil
	}

	// Parallel download with up to 32 concurrent workers.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(32)
	var copied atomic.Int64

	for _, attrs := range objects {
		attrs := attrs
		g.Go(func() error {
			name := path.Base(attrs.Name)
			dst := filepath.Join(dstDir, name)

			// Skip if already exists and same size.
			if info, statErr := os.Stat(dst); statErr == nil && info.Size() == attrs.Size {
				copied.Add(1)
				return nil
			}

			reader, rErr := b.client.Bucket(b.bucket).Object(attrs.Name).NewReader(gctx)
			if rErr != nil {
				return fmt.Errorf("cachepersist: open gcs org object %s: %w", attrs.Name, rErr)
			}
			wErr := copyReaderAtomic(reader, dst, 0o640)
			_ = reader.Close()
			if wErr != nil {
				return fmt.Errorf("cachepersist: hydrate org %s: %w", name, wErr)
			}
			copied.Add(1)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return int(copied.Load()), err
	}
	return int(copied.Load()), nil
}

func (b *gcsBackend) uploadFileToObject(ctx context.Context, srcPath, objName string) error {
	input, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer input.Close()

	writer := b.client.Bucket(b.bucket).Object(objName).NewWriter(ctx)
	writer.ContentType = "application/octet-stream"
	if _, err := io.Copy(writer, input); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
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
