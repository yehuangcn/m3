	"github.com/m3db/m3db/storage/namespace"
	xtime "github.com/m3db/m3x/time"
	Open(namespace ts.ID, blockSize time.Duration, shard uint32, start time.Time) error
	Open(md namespace.Metadata) error
	Open(md namespace.Metadata) error
	// SetInfoReaderBufferSize sets the buffer size for reading TSDB info, digest and checkpoint files
	SetInfoReaderBufferSize(value int) Options

	// InfoReaderBufferSize returns the buffer size for reading TSDB info, digest and checkpoint files
	InfoReaderBufferSize() int

	// SetDataReaderBufferSize sets the buffer size for reading TSDB data and index files
	SetDataReaderBufferSize(value int) Options

	// DataReaderBufferSize returns the buffer size for reading TSDB data and index files
	DataReaderBufferSize() int

	// SetSeekReaderBufferSize size sets the buffer size for seeking TSDB files
	SetSeekReaderBufferSize(value int) Options
	// SeekReaderBufferSize size returns the buffer size for seeking TSDB files
	SeekReaderBufferSize() int