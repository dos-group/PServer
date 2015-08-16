package de.tuberlin.pserver.types;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import de.tuberlin.pserver.commons.json.GsonUtils;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.utils.VectorFunction;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.SlotContext;

public class DistributedMatrix extends AbstractMatrix {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    class PartitionShape {

        private transient Gson gson = GsonUtils.createPrettyPrintAndAnnotationExclusionGson();

        final long rows;
        final long cols;
        final long rowOffset;
        final long colOffset;

        public PartitionShape(long rows, long cols) {
            this(rows, cols, 0, 0);
        }

        public PartitionShape(long rows, long cols, long rowOffset, long colOffset) {
            this.rows = rows;
            this.cols = cols;
            this.rowOffset = rowOffset;
            this.colOffset = colOffset;
        }

        public long getRows() {
            return rows;
        }

        public long getCols() {
            return cols;
        }

        public long getRowOffset() {
            return rowOffset;
        }

        public long getColOffset() {
            return colOffset;
        }

        public PartitionShape create(long row, long col) {
            return new PartitionShape(row, col);
        }

        public boolean contains(long row, long col) {
            return row < rows && col < cols;
        }

        @Override
        public String toString() {
            return "\nPartitionShape " + gson.toJson(this);
        }
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final SlotContext slotContext;

    private final int nodeDOP, nodeID;

    private final PartitionType partitionType;

    private final PartitionShape shape;

    private final Format format;

    private final Matrix matrix;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedMatrix(final DistributedMatrix m) {
        this(m.slotContext, m.rows, m.cols, m.partitionType, m.layout, m.format);
    }

    public DistributedMatrix(final SlotContext slotContext,
                             final long rows, final long cols,
                             final PartitionType partitionType,
                             final Layout layout,
                             final Format format) {

        super(rows, cols, layout);

        this.slotContext    = Preconditions.checkNotNull(slotContext);
        this.nodeDOP        = slotContext.programContext.nodeDOP;
        this.nodeID         = slotContext.programContext.runtimeContext.nodeID;
        this.partitionType  = Preconditions.checkNotNull(partitionType);
        this.shape          = createShape(rows, cols);
        this.format         = format;

        this.matrix = new MatrixBuilder()
                .dimension(shape.rows, shape.cols)
                .layout(layout)
                .format(format)
                .build();

        System.out.println(shape.toString());
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private PartitionShape createShape(final long rows, final long cols) {
        switch (partitionType) {
            case NOT_PARTITIONED: return new PartitionShape(rows, cols, 0, 0);
            case ROW_PARTITIONED: {
                final long _rows = (rows % 2 != 0 && nodeID == nodeDOP - 1) ? (rows / nodeDOP) + 1 : rows / nodeDOP;
                return new PartitionShape(_rows, cols, rows / nodeDOP * nodeID, 0);
            }
            case COLUMN_PARTITIONED: throw new UnsupportedOperationException();
            case BLOCK_PARTITIONED: throw new UnsupportedOperationException();
            default: throw new IllegalStateException();
        }
    }

    public long translateRow(final long row) {
        switch (partitionType) {
            case ROW_PARTITIONED: return row - shape.rowOffset;
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
            @Override public boolean hasNextRow() { return iter.hasNextRow(); }
            @Override public void nextRow() { iter.nextRow(); }
            @Override public void nextRandomRow() { iter.nextRandomRow(); }
            @Override public double getValueOfColumn(int col) { return iter.getValueOfColumn(col); }
            @Override public Vector getAsVector() { return iter.getAsVector(); }
            @Override public Vector getAsVector(int from, int size) { return iter.getAsVector(from, size); }
            @Override public void reset() { iter.reset(); }
            @Override public long numRows() { return iter.numRows(); }
            @Override public long numCols() { return iter.numCols(); }
            @Override public int getCurrentRowNum() { return iter.getCurrentRowNum() + (int)shape.rowOffset; }
        };
    }

    // ---------------------------------------------------
    // Public Methods..
    // ---------------------------------------------------

    public long baseRow() { return shape.rowOffset; }

    public long baseCol() { return shape.colOffset; }

    public long partitionedNumRows() { return shape.rows; }

    public long partitionedNumCols() { return shape.cols; }

    @Override public double get(long row, long col) { return matrix.get(translateRow(row), translateCol(col)); }

    @Override public void set(long row, long col, double value) { matrix.set(translateRow(row), translateCol(col), value); }

    @Override public double atomicGet(long row, long col) { return 0; }

    @Override public void atomicSet(long row, long col, double value) {}

    @Override public Vector rowAsVector() { return matrix.rowAsVector(); }

    @Override public Vector rowAsVector(long row) { return matrix.rowAsVector(translateRow(row)); }

    @Override public Vector rowAsVector(long row, long from, long to) { return matrix.rowAsVector(translateRow(row), from, to); }

    @Override public Vector colAsVector() { return matrix.colAsVector(); }

    @Override public Vector colAsVector(long col) { return matrix.colAsVector(translateCol(col)); }

    @Override public Vector colAsVector(long col, long from, long to) { return matrix.colAsVector(translateCol(col), from, to); }

    @Override public Matrix assign(Matrix m) { return matrix.assign(m); }

    @Override public Matrix assign(double v) { return matrix.assign(v); }

    @Override public Matrix assignRow(long row, Vector v) { return assignRow(translateRow(row), v); }

    @Override public Matrix assignColumn(long col, Vector v) { return assignColumn(translateCol(col), v); }

    @Override public Matrix copy() { return new DistributedMatrix(this); }

    @Override protected Matrix newInstance(long rows, long cols) { throw new UnsupportedOperationException(); }

    @Override public double[] toArray() { return matrix.toArray(); }

    @Override public void setArray(double[] data) { matrix.setArray(data); }

    @Override
    public RowIterator rowIterator() {
        return createLocalRowIterator(matrix.rowIterator(
                        (int)translateRow(shape.rowOffset),
                        (int)(translateRow(shape.rowOffset) + shape.rows - 1)
                )
        );
    }

    @Override
    public RowIterator rowIterator(int startRow, int endRow) {
        final int localStart = startRow - (int)shape.rowOffset;
        final int length = endRow - startRow - 1;
        return createLocalRowIterator(
                matrix.rowIterator(
                        (int)translateRow(shape.rowOffset) + localStart,
                        (int)translateRow(shape.rowOffset) + localStart + length
                )
        );
    }

    // ---------------------------------------------------
    // Distributed Operations.
    // ---------------------------------------------------

    public Vector aggregateRows(final VectorFunction f) {
        switch (partitionType) {
            case ROW_PARTITIONED: {

                final Vector partialAgg = new VectorBuilder().dimension(shape.rows).layout(layout).format(format).build();
                final Vector totalAgg = new VectorBuilder().dimension(rows).layout(layout).format(format).build();
                final RowIterator iter = rowIterator();
                while(iter.hasNextRow()) {
                    iter.nextRow();
                    final int row = iter.getCurrentRowNum();
                    final double value = f.apply(rowAsVector(row));
                    partialAgg.set(row - shape.rowOffset, value);
                    totalAgg.set(row, value);
                }

                slotContext.programContext.runtimeContext.dataManager.pushTo("rowAgg", partialAgg);
                final int n = slotContext.programContext.nodeDOP - 1;
                slotContext.programContext.runtimeContext.dataManager.awaitEvent(ExecutionManager.CallType.SYNC, n, "rowAgg",
                        new DataManager.DataEventHandler() {
                            @Override
                            public void handleDataEvent(int srcNodeID, Object value) {
                                final Vector remotePartialAgg = (Vector)value;
                                final long offset = rows / slotContext.programContext.nodeDOP * srcNodeID;
                                for (int i = 0; i < remotePartialAgg.length(); ++i)
                                   totalAgg.set(offset + i, remotePartialAgg.get(i));
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
}
