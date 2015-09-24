package de.tuberlin.pserver.types;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.utils.MatrixAggregation;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.runtime.partitioning.MatrixByRowPartitioner;

public class DistributedMatrix extends AbstractMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    //private final int nodeDOP, nodeID;

    private final int[] atNodes;

    private final PartitionType partitionType;

    private final PartitionShape shape;

    private final Format format;

    private final Matrix matrix;

    public final boolean completeMatrix;
    
    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedMatrix(final DistributedMatrix m) {
        this(m.programContext, m.rows, m.cols, m.atNodes, m.partitionType, m.layout, m.format, m.completeMatrix);
    }

    public DistributedMatrix(final ProgramContext programContext,
                             final long rows, final long cols,
                             final int[] atNodes,
                             final PartitionType partitionType,
                             final Layout layout,
                             final Format format,
                             final boolean completeMatrix) {

        super(rows, cols, layout);

        this.programContext = Preconditions.checkNotNull(programContext);
        //this.nodeDOP        = programContext.nodeDOP;
        //this.nodeID         = programContext.runtimeContext.nodeID;
        this.atNodes        = Preconditions.checkNotNull(atNodes);
        this.partitionType  = Preconditions.checkNotNull(partitionType);
        this.shape          = createShape(rows, cols);
        this.format         = format;
        this.completeMatrix = completeMatrix;

        final long _rows = completeMatrix ? rows : shape.rows;
        final long _cols = completeMatrix ? cols : shape.cols;
        this.matrix  = new MatrixBuilder()
                .dimension(_rows, _cols)
                .layout(layout)
                .format(format)
                .build();
    }

    // ---------------------------------------------------
    // Public Methods..
    // ---------------------------------------------------

    public long partitionBaseRowOffset() { return shape.rowOffset; }

    public long partitionBaseColOffset() { return shape.colOffset; }

    public long partitionNumRows() { return shape.rows; }

    public long partitionNumCols() { return shape.cols; }

    public Matrix.PartitionShape getPartitionShape() { return shape; }

    @Override public double get(long index) { return matrix.get(index); }

    @Override public double get(long row, long col) { return matrix.get(translateRow(row), translateCol(col)); }

    @Override public void set(long row, long col, double value) { matrix.set(translateRow(row), translateCol(col), value); }

    @Override public Matrix getRow(long row) { return matrix.getRow(translateRow(row)); }

    @Override public Matrix getRow(long row, long from, long to) { return matrix.getRow(translateRow(row), from, to); }

    @Override public Matrix getCol(long col) { return matrix.getCol(translateCol(col)); }

    @Override public Matrix getCol(long col, long from, long to) { return matrix.getCol(translateCol(col), from, to); }

    @Override public Matrix assign(Matrix m) { return matrix.assign(m); }

    @Override public Matrix assign(double v) { return matrix.assign(v); }

    @Override public Matrix assignRow(long row, Matrix v) { return matrix.assignRow(translateRow(row), v); }

    @Override public Matrix assignColumn(long col, Matrix v) { return matrix.assignColumn(translateCol(col), v); }

    @Override public Matrix copy() { return new DistributedMatrix(this); }

    @Override public Matrix subMatrix(long row, long col, long rowSize, long colSize) { return null; }

    @Override public Matrix assign(long row, long col, Matrix m) { return null; }

    @Override protected Matrix newInstance(long rows, long cols) { throw new UnsupportedOperationException(); }

    @Override public double[] toArray() { return matrix.toArray(); }

    @Override public void setArray(double[] data) { matrix.setArray(data); }

    @Override
    public RowIterator rowIterator() {
        final int start  = (int)translateRow(shape.rowOffset);
        final int end    = (int)(start + shape.rows);
        return createLocalRowIterator(matrix.rowIterator(start, end));
    }

    @Override
    public RowIterator rowIterator(int startRow, int endRow) {
        final int start  = (int)translateRow(startRow);
        final int end    = (int)translateRow(endRow);
        return createLocalRowIterator(matrix.rowIterator(start, end));
    }

    // ---------------------------------------------------
    // Distributed Operations - Synchronous!
    // ---------------------------------------------------

    public Matrix aggregateRows(final MatrixAggregation f) {
        switch (partitionType) {
            case ROW_PARTITIONED: {

                final Matrix partialAgg = new MatrixBuilder().dimension(1, shape.rows).layout(layout).format(format).build();
                final Matrix totalAgg = new MatrixBuilder().dimension(1, rows).layout(layout).format(format).build();
                final RowIterator iter = rowIterator();
                while(iter.hasNext()) {
                    iter.next();
                    final int row = iter.rowNum();
                    final double value = f.apply(getRow(row));
                    partialAgg.set(0, row - shape.rowOffset, value);
                    totalAgg.set(0, row, value);
                }

                programContext.runtimeContext.dataManager.pushTo("rowAgg", partialAgg);
                final int n = programContext.nodeDOP - 1;
                programContext.runtimeContext.dataManager.receive(DataManager.CallType.SYNC, n, "rowAgg",
                        new DataManager.DataEventHandler() {
                            @Override
                            public void handleDataEvent(int srcNodeID, Object value) {
                                final Matrix remotePartialAgg = (Matrix) value;
                                final long offset = rows / programContext.nodeDOP * srcNodeID;
                                for (int i = 0; i < remotePartialAgg.rows(); ++i)
                                    totalAgg.set(0, offset + i, remotePartialAgg.get(0, i));
                            }
                        }
                );
                return totalAgg;
            }
            case COLUMN_PARTITIONED: throw new IllegalStateException();
            case BLOCK_PARTITIONED: throw new IllegalStateException();
            default: throw new IllegalStateException();
        }
    }

    public DistributedMatrix collectRemotePartitions() {
        if (!completeMatrix)
            throw new IllegalStateException();
        switch (partitionType) {
            case ROW_PARTITIONED: {
                Matrix partialMatrix = matrix.subMatrix(shape.rowOffset, shape.colOffset, shape.rows, shape.cols);
                programContext.runtimeContext.dataManager.pushTo("partialMatrix", partialMatrix);
                final int n = programContext.nodeDOP - 1;
                programContext.runtimeContext.dataManager.receive(DataManager.CallType.SYNC, n, "partialMatrix",
                        new DataManager.DataEventHandler() {
                            @Override
                            public void handleDataEvent(int srcNodeID, Object value) {
                                final Matrix remotePartialMatrix = (Matrix) value;
                                matrix.assign(srcNodeID * shape.rows, 0, remotePartialMatrix);
                            }
                        }
                );
            } break;
            case COLUMN_PARTITIONED: throw new IllegalStateException();
            case BLOCK_PARTITIONED: throw new IllegalStateException();
            default: throw new IllegalStateException();
        }
        return this;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private PartitionShape createShape(final long rows, final long cols) {
        switch (partitionType) {
            case NOT_PARTITIONED: return new PartitionShape(rows, cols, 0, 0);
            case ROW_PARTITIONED: {
                return new MatrixByRowPartitioner(programContext.runtimeContext.nodeID, atNodes.length, rows, cols).getPartitionShape();
            }
            case COLUMN_PARTITIONED: throw new UnsupportedOperationException();
            case BLOCK_PARTITIONED: throw new UnsupportedOperationException();
            default: throw new IllegalStateException();
        }
    }

    public long translateRow(final long row) {
        switch (partitionType) {
            case ROW_PARTITIONED: return completeMatrix ? row : row - shape.rowOffset;
            case COLUMN_PARTITIONED: throw new UnsupportedOperationException();
            case BLOCK_PARTITIONED: throw new UnsupportedOperationException();
            default: throw new IllegalStateException();
        }
    }

    public long translateCol(final long col) {
        switch (partitionType) {
            case ROW_PARTITIONED: return col;
            case COLUMN_PARTITIONED: throw new IllegalStateException();
            case BLOCK_PARTITIONED: throw new IllegalStateException();
            default: throw new IllegalStateException();
        }
    }

    public Matrix.RowIterator createLocalRowIterator(final Matrix.RowIterator iter) {
        return new Matrix.RowIterator() {
            @Override public boolean hasNext() { return iter.hasNext(); }
            @Override public void next() { iter.next(); }
            @Override public void nextRandom() { iter.nextRandom(); }
            @Override public double value(long col) { return iter.value(col); }
            @Override public Matrix get() { return iter.get(); }
            @Override public Matrix get(int from, int size) { return iter.get(from, size); }
            @Override public void reset() { iter.reset(); }
            @Override public long rows() { return iter.rows(); }
            @Override public long cols() { return iter.cols(); }
            @Override public int rowNum() { return completeMatrix
                    ? iter.rowNum()
                    : iter.rowNum() + (int)shape.rowOffset; }
        };
    }
}
