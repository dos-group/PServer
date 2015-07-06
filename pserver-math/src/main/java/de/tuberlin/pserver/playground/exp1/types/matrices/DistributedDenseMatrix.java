package de.tuberlin.pserver.playground.exp1.types.matrices;

public final class DistributedDenseMatrix /*implements Matrix*/ {
/*
    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class MatrixDescriptor implements Serializable {

        public final long baseRowOffset;

        public final long baseColOffset;

        public final long partitionRows;

        public final long partitionCols;

        public final Types.TypeInformation typeInfo;

        public final PartitioningScheme partitioningScheme;

        public MatrixDescriptor(final long baseRowOffset,
                                final long baseColOffset,
                                final long partitionRows,
                                final long partitionCols,
                                final Types.TypeInformation elementTypeInfo,
                                final PartitioningScheme partitioningScheme) {

            this.baseRowOffset = baseRowOffset;
            this.baseColOffset = baseColOffset;
            this.partitionRows = partitionRows;
            this.partitionCols = partitionCols;
            this.typeInfo      = elementTypeInfo;
            this.partitioningScheme = Preconditions.checkNotNull(partitioningScheme);
        }
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static enum PartitioningScheme {

        ROW_PARTITIONING,

        COLUMN_PARTITIONING,

        BLOCK_PARTITIONING;
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DistributedDenseMatrix.class);

    private final long rows;

    private final long cols;

    private final PartitioningScheme partitioningScheme;

    private final Matrix.BlockLayout blockLayout;

    private final Types.TypeInformation elementTypeInfo;

    private Key matrixKey;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private DistributedDenseMatrix(final Key matrixKey,
                                   final long rows, final long cols,
                                   final Types.TypeInformation elementTypeInfo,
                                   final PartitioningScheme partitioningScheme,
                                   final Matrix.BlockLayout blockLayout) {

        this.matrixKey = matrixKey;
        this.rows = rows;
        this.cols = cols;
        this.elementTypeInfo = Preconditions.checkNotNull(elementTypeInfo);
        this.partitioningScheme = Preconditions.checkNotNull(partitioningScheme);
        this.blockLayout = blockLayout;
    }

    // ---------------------------------------------------

    public static DistributedDenseMatrix create(final Key matrixKey,
                                           final long rows, final long cols,
                                           final Types.TypeInformation elementTypeInfo,
                                           final PartitioningScheme partitioningScheme) {
        return create(matrixKey, rows, cols, elementTypeInfo, partitioningScheme, null);
    }
    public static DistributedDenseMatrix create(final Key matrixKey,
                                           final long rows, final long cols,
                                           final Types.TypeInformation elementTypeInfo,
                                           final PartitioningScheme partitioningScheme,
                                           final Matrix.BlockLayout blockLayout) {

        if (partitioningScheme == PartitioningScheme.BLOCK_PARTITIONING)
            if (blockLayout == null)
                throw new IllegalArgumentException();

        return new DistributedDenseMatrix(
                matrixKey,
                rows, cols,
                Preconditions.checkNotNull(elementTypeInfo),
                Preconditions.checkNotNull(partitioningScheme),
                blockLayout
        ).distribute();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public long numRows() { return rows; };

    @Override
    public long numCols() { return cols; }

    @Override
    public Types.TypeInformation getType() { return null; }

    @Override
    public Types.TypeInformation getElementType() { return elementTypeInfo; }

    @Override
    public TypedBuffer getBuffer() { return null; }

    @Override
    public byte[] getRows(final long row) { throw new NotImplementedException(); }

    @Override
    public byte[] getColumn(final long col) { throw new NotImplementedException(); }

    @Override
    public byte[] getElement(final long row, final long col) {
        final long globalOffset = getGlobalOffset(row, col);
        final int segmentID = (int)globalOffset / ByteBufferValue.DEFAULT_SEGMENT_SIZE;
        final int localElementPos = (int)((globalOffset - segmentID * ByteBufferValue.DEFAULT_SEGMENT_SIZE) / elementTypeInfo.length());
        final ByteBufferValue.Segment[] segments = DHT.getInstance().get(matrixKey, segmentID);
        return new TypedBuffer(segments[0].data, elementTypeInfo).extractElementAsByteArray(localElementPos);
    }

    @Override
    public void setElement(final long row, final long col, final byte[] element) {
        final long globalOffset = getGlobalOffset(row, col);
        final int segmentID = matrixKey.getSegmentIDFromByteOffset(globalOffset);
        final ByteBufferValue.Segment[] segments = DHT.getInstance().get(matrixKey, segmentID);
        final int localElementPos = (int)((globalOffset - segmentID * ByteBufferValue.DEFAULT_SEGMENT_SIZE) / elementTypeInfo.length());
        new TypedBuffer(segments[0].data, elementTypeInfo).put(localElementPos, element);
        DHT.getInstance().put(matrixKey, segments);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private ByteBufferValue[] computeMatrixPartitions() {
        final long length = elementTypeInfo.length() * rows * cols;

        long alignmentSize = 0;
        switch (partitioningScheme) {
            case ROW_PARTITIONING:      alignmentSize = cols * elementTypeInfo.length(); break;
            case COLUMN_PARTITIONING:   alignmentSize = rows * elementTypeInfo.length(); break;
            case BLOCK_PARTITIONING:    throw new IllegalStateException();
        }

        final ByteBufferValue[] matrixValues = ByteBufferValue.newValueAligned(false, length, alignmentSize);
        final MatrixDescriptor[] matrixDescriptors = new MatrixDescriptor[matrixValues.length];
        long partitionRows;
        long partitionCols;

        switch (partitioningScheme) {

            case ROW_PARTITIONING:  {
                partitionRows = rows / matrixDescriptors.length;
                partitionCols = cols;
                if (partitionRows * cols > Integer.MAX_VALUE)
                    throw new IllegalStateException();
                for (int i = 0; i < matrixDescriptors.length; ++i)
                    matrixDescriptors[i] = new MatrixDescriptor(partitionRows * i, 0,
                            partitionRows, partitionCols, elementTypeInfo, PartitioningScheme.ROW_PARTITIONING);
            } break;

            case COLUMN_PARTITIONING: {
                partitionRows = rows;
                partitionCols = cols / matrixDescriptors.length;
                if (partitionCols * rows > Integer.MAX_VALUE)
                    throw new IllegalStateException();
                for (int i = 0; i < matrixDescriptors.length; ++i)
                    matrixDescriptors[i] = new MatrixDescriptor(0, partitionCols * i,
                            partitionRows, partitionCols, elementTypeInfo, PartitioningScheme.COLUMN_PARTITIONING);
            } break;

            case BLOCK_PARTITIONING: {
                throw new IllegalStateException();
                //if (matrixValues.length % 4 != 0)
                //    throw new IllegalStateException();
                //partitionRows = rows / matrixDescriptors.length;
                //partitionCols = cols / matrixDescriptors.length;
                //for (int i = 0; i < matrixDescriptors.length; ++i)
                //    matrixDescriptors[i] = new MatrixDescriptor(partitionRows * i, partitionCols * i,
                //            partitionRows, partitionCols, elementTypeInfo, PartitioningScheme.BLOCK_PARTITIONING);
            }
        }

        for (int i = 0; i < matrixValues.length; ++i)
            matrixValues[i].setValueMetadata(matrixDescriptors[i]);

        return matrixValues;
    }

    private long getGlobalOffset(final long row, final long col) { return getGlobalOffset(row, col, 0, 0, 0, 0); }
    private long getGlobalOffset(final long row, final long col,
                                 final int col_length, final int row_length,
                                 final int row_blocks, final int col_blocks) {

        switch (partitioningScheme) {
            case ROW_PARTITIONING:    return (row * cols + col) * elementTypeInfo.length();
            case COLUMN_PARTITIONING: return (col * rows + row) * elementTypeInfo.length();
            case BLOCK_PARTITIONING:  {
                switch (blockLayout) {
                    case ROW_LAYOUT: {
                        final long block_base = (col / col_length) * (row_length * col_length) +
                                ((row / row_length) * (row_length * col_length) * col_blocks);
                        return (col % col_length) + ((row % row_length) * col_length) + block_base;
                    }
                    case COLUMN_LAYOUT: {
                        final long block_base = (row / row_length) * (col_length * row_length) +
                                ((col / col_length) * (col_length * row_length) * row_blocks);
                        return (row % row_length) + ((col % col_length) * row_length) + block_base;
                    }
                }
            }
            default: throw new IllegalStateException();
        }
    }

    private DistributedDenseMatrix distribute() {
        final ByteBufferValue[] matrixValues = computeMatrixPartitions();
        matrixKey = DHT.getInstance().put(matrixKey, matrixValues);
        return this;
    }*/
}
