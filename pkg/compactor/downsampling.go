package compactor

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/bucket"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type DownsampleMetrics struct {
	downsamples             *prometheus.CounterVec
	downsampleFailures      *prometheus.CounterVec
	blocksMarkedForDeletion prometheus.Counter
}

func newDownsampleMetrics(reg *prometheus.Registry) *DownsampleMetrics {
	m := new(DownsampleMetrics)

	m.downsamples = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_total",
		Help: "Total number of downsampling attempts.",
	}, []string{"group"})
	m.downsampleFailures = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_compact_downsample_failures_total",
		Help: "Total number of failed downsampling attempts.",
	}, []string{"group"})
	m.blocksMarkedForDeletion = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_compactor_downsampling_blocks_marked_for_deletion_total",
		Help: "Total number of blocks marked for deletion in compactor downsampling.",
	})
	return m
}

var downsampleMetrics *DownsampleMetrics

type Downsampler struct {
	ulogger         log.Logger
	syncer          *compact.Syncer
	bucket          *bucket.UserBucketClient
	reg             *prometheus.Registry
	downsamplingDir string
	maxLevel        int64
	ds5m            int64
	ds1h            int64
}

func NewDownsamper(ulogger log.Logger, syncer *compact.Syncer, bucket *bucket.UserBucketClient, reg *prometheus.Registry, downsamplingDir string, maxLevel, ds5m, ds1h int64) *Downsampler {
	return &Downsampler{
		ulogger:         ulogger,
		syncer:          syncer,
		bucket:          bucket,
		reg:             reg,
		downsamplingDir: downsamplingDir,
		maxLevel:        maxLevel,
		ds5m:            ds5m,
		ds1h:            ds1h,
	}
}

func (d *Downsampler) Downsample(ctx context.Context) error {
	// After all compactions are done, work down the downsampling backlog.
	// We run two passes of this to ensure that the 1h downsampling is generated
	// for 5m downsamplings created in the first run.
	level.Info(d.ulogger).Log("msg", "start first pass of downsampling")
	if err := d.syncer.SyncMetas(ctx); err != nil {
		return errors.Wrap(err, "sync before first pass of downsampling")
	}
	downsampleMetrics = newDownsampleMetrics(d.reg)
	for _, meta := range d.syncer.Metas() {
		groupKey := compact.DefaultGroupKey(meta.Thanos)
		downsampleMetrics.downsamples.WithLabelValues(groupKey)
		downsampleMetrics.downsampleFailures.WithLabelValues(groupKey)
	}
	if err := downsampleBucket(ctx, d.ulogger, downsampleMetrics, d.bucket, d.syncer.Metas(), d.downsamplingDir); err != nil {
		return errors.Wrap(err, "first pass of downsampling failed")
	}

	level.Info(d.ulogger).Log("msg", "start second pass of downsampling")
	if err := d.syncer.SyncMetas(ctx); err != nil {
		return errors.Wrap(err, "sync before second pass of downsampling")
	}
	if err := downsampleBucket(ctx, d.ulogger, downsampleMetrics, d.bucket, d.syncer.Metas(), d.downsamplingDir); err != nil {
		return errors.Wrap(err, "second pass of downsampling failed")
	}
	level.Info(d.ulogger).Log("msg", "downsampling iterations done")
	return nil
}

func downsampleBucket(
	ctx context.Context,
	logger log.Logger,
	metrics *DownsampleMetrics,
	bkt objstore.Bucket,
	metas map[ulid.ULID]*metadata.Meta,
	dir string,
) error {
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrap(err, "clean working directory")
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return errors.Wrap(err, "create dir")
	}

	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			level.Error(logger).Log("msg", "failed to remove downsample cache directory", "path", dir, "err", err)
		}
	}()

	// mapping from a hash over all source IDs to blocks. We don't need to downsample a block
	// if a downsampled version with the same hash already exists.
	sources5m := map[ulid.ULID]struct{}{}
	sources1h := map[ulid.ULID]struct{}{}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case downsample.ResLevel0:
			continue
		case downsample.ResLevel1:
			for _, id := range m.Compaction.Sources {
				sources5m[id] = struct{}{}
			}
		case downsample.ResLevel2:
			for _, id := range m.Compaction.Sources {
				sources1h[id] = struct{}{}
			}
		default:
			return errors.Errorf("unexpected downsampling resolution %d", m.Thanos.Downsample.Resolution)
		}
	}

	for _, m := range metas {
		switch m.Thanos.Downsample.Resolution {
		case downsample.ResLevel0:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources5m[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only minTime >= 7d delay downsampling 5m
			if m.MinTime > time.Now().Add(-3*24*time.Hour).Unix()*1000 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, downsample.ResLevel1); err != nil {
				metrics.downsampleFailures.WithLabelValues(compact.DefaultGroupKey(m.Thanos)).Inc()
				return errors.Wrap(err, "downsampling to 5 min")
			}
			metrics.downsamples.WithLabelValues(compact.DefaultGroupKey(m.Thanos)).Inc()

		case downsample.ResLevel1:
			missing := false
			for _, id := range m.Compaction.Sources {
				if _, ok := sources1h[id]; !ok {
					missing = true
					break
				}
			}
			if !missing {
				continue
			}
			// Only minTime >= 30d delay downsampling 1h
			if m.MinTime > time.Now().Add(-5*24*time.Hour).Unix()*1000 {
				continue
			}
			if err := processDownsampling(ctx, logger, bkt, m, dir, downsample.ResLevel2); err != nil {
				metrics.downsampleFailures.WithLabelValues(compact.DefaultGroupKey(m.Thanos)).Inc()
				return errors.Wrap(err, "downsampling to 60 min")
			}
			metrics.downsamples.WithLabelValues(compact.DefaultGroupKey(m.Thanos)).Inc()
		}
	}
	return nil
}

func processDownsampling(ctx context.Context, logger log.Logger, bkt objstore.Bucket, m *metadata.Meta, dir string, resolution int64) error {
	begin := time.Now()
	bdir := filepath.Join(dir, m.ULID.String())

	err := block.Download(ctx, logger, bkt, m.ULID, bdir)
	if err != nil {
		return errors.Wrapf(err, "download block %s", m.ULID)
	}
	level.Info(logger).Log("msg", "downloaded block", "id", m.ULID, "duration", time.Since(begin))

	if err := block.VerifyIndex(logger, filepath.Join(bdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil {
		return errors.Wrap(err, "input block index not valid")
	}

	begin = time.Now()

	var pool chunkenc.Pool
	if m.Thanos.Downsample.Resolution == 0 {
		pool = chunkenc.NewPool()
	} else {
		pool = downsample.NewPool()
	}

	b, err := tsdb.OpenBlock(logger, bdir, pool)
	if err != nil {
		return errors.Wrapf(err, "open block %s", m.ULID)
	}
	defer runutil.CloseWithLogOnErr(log.With(logger, "outcome", "potential left mmap file handlers left"), b, "tsdb reader")

	id, err := downsample.Downsample(logger, m, b, dir, resolution)
	if err != nil {
		return errors.Wrapf(err, "downsample block %s to window %d", m.ULID, resolution)
	}
	resdir := filepath.Join(dir, id.String())

	level.Info(logger).Log("msg", "downsampled block",
		"from", m.ULID, "to", id, "duration", time.Since(begin))

	if err := block.VerifyIndex(logger, filepath.Join(resdir, block.IndexFilename), m.MinTime, m.MaxTime); err != nil {
		return errors.Wrap(err, "output block index not valid")
	}

	begin = time.Now()

	err = block.Upload(ctx, logger, bkt, resdir)
	if err != nil {
		return errors.Wrapf(err, "upload downsampled block %s", id)
	}

	level.Info(logger).Log("msg", "uploaded block", "id", id, "duration", time.Since(begin))

	// Spawn a new context so we always mark a block for deletion in full on shutdown.
	delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	level.Info(logger).Log("msg", "marking downsampling block for deletion", "old_block", id)
	if err := block.MarkForDeletion(delCtx, logger, bkt, m.ULID, "source of downsampling block", downsampleMetrics.blocksMarkedForDeletion); err != nil {
		return errors.Wrapf(err, "mark block %s for deletion from bucket", id)
	}

	// It is not harmful if these fails.
	if err := os.RemoveAll(bdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "dir", bdir, "err", err)
	}
	if err := os.RemoveAll(resdir); err != nil {
		level.Warn(logger).Log("msg", "failed to clean directory", "resdir", bdir, "err", err)
	}

	return nil
}
