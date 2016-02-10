package de.tuberlin.pserver.types.matrix.f32.dense;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.types.InternalData;
import de.tuberlin.pserver.types.matrix.AbstractDistributedMatrixType;
import de.tuberlin.pserver.types.matrix.f32.Matrix32F;
import de.tuberlin.pserver.types.matrix.f32.operations.BinaryOperator32;
import de.tuberlin.pserver.types.matrix.f32.operations.MatrixAggregation32;
import de.tuberlin.pserver.types.matrix.f32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.types.matrix.f32.operations.UnaryOperator32;
import de.tuberlin.pserver.types.matrix.f32.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.matrix.partitioner.PartitionType;

import java.util.Arrays;
import java.util.Random;

public class DenseMatrix32F extends AbstractDistributedMatrixType implements Matrix32F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public float[] data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseMatrix32F(DenseMatrix32F toCopy) {
        super(toCopy.nodeID, toCopy.nodes, toCopy.partitioner.getPartitionType(), toCopy.globalRows, toCopy.globalCols);
        this.data = (data == null) ? new float[(int)(rows() * cols())] : Preconditions.checkNotNull(data);
    }

    public DenseMatrix32F(long globalRows, long globalCols) {
        this(-1, null, PartitionType.NO_PARTITIONER, globalRows, globalCols, null);
    }

    public DenseMatrix32F(long globalRows, long globalCols, final float[] data) {
        this(-1, null, PartitionType.NO_PARTITIONER, globalRows, globalCols, data);
    }

    public DenseMatrix32F(int nodeID, int[] nodes, PartitionType partitionType, long globalRows, long globalCols, final float[] data) {
        super(nodeID, nodes, partitionType, globalRows, globalCols);
        this.data = (data == null) ? new float[(int)(rows() * cols())] : Preconditions.checkNotNull(data);
    }

    // ---------------------------------------------------
    // Distributed Type Metadata.
    // ---------------------------------------------------

    @Override public long sizeOf() { return shape.rows * shape.cols * Float.BYTES; }

    @Override public long globalSizeOf() { return globalRows * globalCols * Float.BYTES; }

    @SuppressWarnings("unchecked")
    @Override public InternalData<float[]> internal() { return new InternalData<>(data); }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Matrix32F copy() {
        return new DenseMatrix32F(this);
    }

    @Override
    public Matrix32F copy(final long rows, final long cols) {
        Matrix32F result = new DenseMatrix32F(rows, cols);
        for (int row = 0; row < Math.min(this.rows(), rows); row++) {
            for (int col = 0; col < Math.min(this.cols(), cols); col++) {
                result.set(row, col, this.get(row, col));
            }
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Matrix32[" + rows() + "|" + cols() + "]: ");
        for (int r = 0; r < rows(); r++) {
            for (int c = 0; c < cols(); c++) {
                stringBuilder.append("(" + r + "," + c + "," + get(r, c) + ") ");
            }
        }
        return stringBuilder.toString();
    }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(final long r, final long c, final float value) {
        try {
            data[(int)(r * cols() + c)] = value;
        } catch(ArrayIndexOutOfBoundsException e) {
            if(r < rows() && c < cols()) {
                throw new IllegalStateException(String.format("Attempt to set a valid position (%d, %d) " +
                                "in matrix of shape (%d, %d) yielded ArrayIndexOutOfBounds: %d",
                        r, c, rows(), cols(), (int)(r * cols() + c)), e);
            }
            throw new IllegalStateException(String.format("Attempt to set a invalid position (%d, %d) " +
                            "in matrix of shape (%d, %d) yielded ArrayIndexOutOfBounds: %d",
                    r, c, rows(), cols(), (int)(r * cols() + c)), e);
        }
    }

    @Override
    public Matrix32F setDiagonalsToZero() {
        return setDiagonalsToZero(this.copy());
    }

    @Override
    public Matrix32F setDiagonalsToZero(final Matrix32F B) {
        long diag = 0;
        while(diag < rows() && diag < cols()) {
            B.set(diag, diag, 0f);
            diag++;
        }
        return B;
    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
    public float get(final long index) {
        return data[(int)index];
    }

    @Override
    public float get(final long row, final long col) {
        return data[(int)(row * cols() + col)];
    }

    @Override
    public Matrix32F getRow(final long row) {
        return getRow(row, 0, cols());
    }

    @Override
    public Matrix32F getRow(final long row, final long from, final long to) {
        // TODO: Optimize with respect to the layout with array copy.
        Matrix32F r = new DenseMatrix32F(1, to - from);
        for (long col = from; col < to; ++col)
            r.set(0, col, data[(int)(row * cols() + col)]);
        return r;
    }

    @Override
    public Matrix32F getCol(final long col) {
        return getCol(col, 0, rows());
    }

    @Override
    public Matrix32F getCol(final long col, final long from, final long to) {
        float[] result = new float[(int)(to - from)];
        for (int i = 0; i < result.length; i++) {
            int row = (int)from + i;
            result[i] = data[(int)(row * cols() + col)];
        }
        return new DenseMatrix32F(result.length, 1, result);
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix32F applyOnElements(final UnaryOperator32 f) {
        return applyOnElements(f, new DenseMatrix32F(rows(), cols()));
    }

    @Override
    public Matrix32F applyOnElements(final UnaryOperator32 f, final Matrix32F B) {
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                B.set(i, j, f.apply(this.get(i, j)));
            }
        }
        return B;
    }

    @Override
    public Matrix32F applyOnElements(final Matrix32F B, final BinaryOperator32 f) {
        return applyOnElements(B, f, new DenseMatrix32F(rows(), cols()));
    }

    @Override
    public Matrix32F applyOnElements(final Matrix32F B, final BinaryOperator32 f, final Matrix32F C) {
        for (int i = 0; i < rows(); ++i) {
            for (int j = 0; j < cols(); ++j) {
                C.set(i, j, f.apply(this.get(i, j), B.get(i, j)));
            }
        }
        return C;
    }

    @Override
    public Matrix32F applyOnElements(final MatrixElementUnaryOperator32 f) {
        return applyOnElements(f, new DenseMatrix32F(rows(), cols()));
    }

    @Override
    public Matrix32F applyOnElements(final MatrixElementUnaryOperator32 f, final Matrix32F B) {
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                B.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return B;
    }

    @Override
    public Matrix32F applyOnNonZeroElements(final MatrixElementUnaryOperator32 f) {
        return applyOnNonZeroElements(f, new DenseMatrix32F(rows(), cols()));
    }

    @Override
    public Matrix32F applyOnNonZeroElements(final MatrixElementUnaryOperator32 f, final Matrix32F B) {
        for (int row = 0; row < rows(); ++row) {
            for (int col = 0; col < cols(); ++col) {
                float oldVal = get(row, col);
                if(oldVal != 0.0) {
                    float newVal = f.apply(row, col, oldVal);
                    if (newVal != oldVal) {
                        B.set(row, col, newVal);
                    }
                }
            }
        }
        return this;
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix32F assign(final Matrix32F v) {
        System.arraycopy(((DenseMatrix32F)v).data, 0, data, 0, (int)(rows() * cols()));
        return this;
    }

    @Override
    public Matrix32F assign(final float v) {
        Arrays.fill(data, v);
        return this;
    }

    @Override
    public Matrix32F assignRow(final long row, final Matrix32F v) {
        for (int col = 0; col < cols(); ++col)
            data[(int)(row * cols() + col)] = v.get(col);
        return this;
    }

    @Override
    public Matrix32F assignColumn(final long col, final Matrix32F v) {
        float[] vData = ((DenseMatrix32F)v).data;
        for (int row = 0; row < rows(); row++) {
            data[(int)(row * cols() + col)] = vData[row];
        }
        //}
        return this;
    }

    @Override
    public Matrix32F assign(final long rowOffset, final long colOffset, final Matrix32F m) {
        //if (layout == Layout.ROW_LAYOUT && m.layout() == layout && cols == m.cols()) {
        System.arraycopy(((DenseMatrix32F)m).data, 0, data, (int) (rowOffset * cols() + colOffset), ((DenseMatrix32F)m).data.length);
        //}
        //else if(layout == Layout.COLUMN_LAYOUT && m.layout() == layout && rows == m.rows()) {
        //    System.arraycopy(m.toArray(), 0, data, (int) (colOffset * rows + rowOffset), ((float[])m.toArray()).length);
        //}
        return null; //assign(rowOffset, colOffset, m);
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public float aggregate(final BinaryOperator32 combiner, final UnaryOperator32 mapper, final Matrix32F result) {
        // return aggregateRows(v -> v.aggregate(combiner, mapper), this).aggregate(combiner, Functions.IDENTITY);
        return 0f;
    }

    @Override
    public Matrix32F aggregateRows(final MatrixAggregation32 f) {
        return aggregateRows(f, new DenseMatrix32F(rows(), 1));
    }

    @Override
    public Matrix32F aggregateRows(final MatrixAggregation32 f, final Matrix32F result) {
        for (int row = 0; row < rows(); row++) {
            result.set(row, 0, f.apply(getRow(row)));
        }
        return result;
    }

    @Override
    public float sum() {
        float sum = 0;
        for (int row = 0; row < rows(); row++) {
            for (int col = 0; col < cols(); col++) {
                sum += this.get(row, col);
            }
        }
        return sum;
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix32F add(final Matrix32F B) {
        return add(B, new DenseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F add(final Matrix32F B, final Matrix32F C) {
        //Utils.checkShapeEqual(this, B, C);
        //return this.applyOnElements(B, (x, y) -> x + y, C);
        if (B.getClass() == Matrix32F.class && C.getClass() == Matrix32F.class) {
            DenseMatrix32F b = (DenseMatrix32F) B;
            DenseMatrix32F c = (DenseMatrix32F) C;
            for (int i = 0; i < data.length; ++i) {
                c.data[i] = data[i] + b.data[i];
            }
        }
        if (B.getClass() == SparseMatrix32F.class && C.getClass() == DenseMatrix32F.class) {
            SparseMatrix32F b = (SparseMatrix32F) B;
            DenseMatrix32F c = (DenseMatrix32F) C;
            b.data.forEachEntry((k,v) -> {
                c.data[(int)k] = data[(int)k] + v;
                return true;
            });
            //applyOnElements(B, (x, y) -> x + y, C);
        }
        return this;
    }

    @Override
    public Matrix32F addVectorToRows(final Matrix32F v) {
        return addVectorToRows(v, new DenseMatrix32F(rows(), cols()));
    }

    @Override
    public Matrix32F addVectorToRows(final Matrix32F v, final Matrix32F B) {
        for (int row = 0; row < rows(); row++) {
            for (int col = 0; col < cols(); col++) {
                B.set(row, col, this.get(row, col) + v.get(col));
            }
        }
        return B;
    }

    @Override
    public Matrix32F addVectorToCols(final Matrix32F v) {
        return addVectorToCols(v, new DenseMatrix32F(rows(), cols()));
    }

    @Override
    public Matrix32F addVectorToCols(final Matrix32F v, final Matrix32F B) {
        for (int col = 0; col < cols(); col++) {
            for (int row = 0; row < rows(); row++) {
                B.set(row, col, this.get(row, col) + v.get(row));
            }
        }
        return B;
    }

    // ----------------------------------------

    @Override
    public Matrix32F sub(final Matrix32F B) {
        return sub(B, new DenseMatrix32F(this.rows(), this.cols()));
    }

    @Override
    public Matrix32F sub(final Matrix32F B, final Matrix32F C) {
        if (B.getClass() == DenseMatrix32F.class && C.getClass() == DenseMatrix32F.class) {
            DenseMatrix32F b = (DenseMatrix32F) B;
            DenseMatrix32F c = (DenseMatrix32F) C;
            for (int i = 0; i < data.length; ++i) {
                c.data[i] = data[i] - b.data[i];
            }
        }
        if (B.getClass() == SparseMatrix32F.class && C.getClass() == DenseMatrix32F.class) {
            SparseMatrix32F b = (SparseMatrix32F) B;
            DenseMatrix32F c = (DenseMatrix32F) C;
            b.data.forEachEntry((k,v) -> {
                c.data[(int)k] = data[(int)k] - v;
                return true;
            });
            //applyOnElements(B, (x, y) -> x + y, C);
        }
        return this;
    }

    // ----------------------------------------

    @Override
    public Matrix32F mul(final Matrix32F B) {
        return mul(B, new DenseMatrix32F(this.rows(), B.cols()));
    }

    @Override
    public Matrix32F mul(final Matrix32F B, final Matrix32F C) {
        for (int row = 0; row < C.rows(); row++) {
            for (int col = 0; col < C.cols(); col++) {
                C.set(row, col, this.getRow(row).dot(B.getCol(col)));
            }
        }
        return C;
    }

    // ----------------------------------------

    @Override
    public Matrix32F scale(final float a) {
        return scale(a, new DenseMatrix32F(rows(), cols()));
    }

    @Override
    public Matrix32F scale(final float a, final Matrix32F B) {
        for (int i = 0; i < data.length; ++i)
            data[i] = data[i] * a;
        return this;
    }

    // ----------------------------------------

    @Override
    public Matrix32F transpose() {
        return transpose(new DenseMatrix32F(cols(), rows()));
    }

    @Override
    public Matrix32F transpose(final Matrix32F B) {
        for (int row = 0; row < rows(); row++) {
            for (int col = 0; col < cols(); col++) {
                B.set(col, row, this.get(row, col));
            }
        }
        return B;
    }

    // ----------------------------------------

    @Override
    public Matrix32F invert() {
        return invert(new DenseMatrix32F(cols(), rows()));
    }

    @Override
    public Matrix32F invert(final Matrix32F B) {
        //LOG.warn("invert() has not been overridden by effective subclass. This implementation does nothing.");
        return B;
    }

    // ----------------------------------------

    @Override
    public float norm(final int p) {
        double norm = 0;
        for (int row = 0; row < rows(); row++) {
            for (int col = 0; col < cols(); col++) {
                norm += Math.pow(this.get(row, col), p);
            }
        }
        return (float)Math.pow(norm, 1./p);
    }

    // ----------------------------------------

    @Override
    public float dot(final Matrix32F B) {
        float result = 0;
        //if(this.layout == Layout.ROW_LAYOUT) {
        //Preconditions.checkArgument(rows == 1);
        //Preconditions.checkArgument(B.layout() == Layout.ROW_LAYOUT);
        //Preconditions.checkArgument(B.rows() == 1);
        //Preconditions.checkArgument(cols == B.cols());

        if (B.getClass() == Matrix32F.class) {
            DenseMatrix32F b = (DenseMatrix32F) B;
            for (int i = 0; i < cols() * rows(); i++) {
                result += this.data[i] * b.data[i];
            }
        }
        if (B.getClass() == SparseMatrix32F.class) {
            for (int i = 0; i < cols() * rows(); i++) {
                result += this.data[i] * B.get(i);
            }
        }
        //}
        /*else if(this.layout == Layout.COLUMN_LAYOUT) {
            Preconditions.checkArgument(cols == 1);
            Preconditions.checkArgument(B.layout() == Layout.COLUMN_LAYOUT);
            Preconditions.checkArgument(B.cols() == 1);
            Preconditions.checkArgument(rows == B.rows());
            for (int row = 0; row < rows; row++) {
                result += this.get(row) * B.get(row);
            }
        }
        else {
            throw new IllegalStateException("Unknown layout: " + layout.name());
        }*/
        return result;
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix32F subMatrix(final long rowOffset, final long colOffset, final long rows, final long cols) {
        //if (layout == Layout.ROW_LAYOUT) {
        final int length = (int)(rows * cols);
        final float[] subData = new float[length];
        try {
            System.arraycopy(data, (int) (rowOffset * cols + colOffset), subData, 0, length);
        }
        catch(ArrayIndexOutOfBoundsException e) {
            throw new RuntimeException(String.format("subMatrix(%d, %d, %d, %d) caused ArrayIndexOfBoundsException", rowOffset, colOffset, rows, cols), e);
        }
        return new DenseMatrix32F(rows, cols, subData);
        //} else
        //    throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32F concat(final Matrix32F B) {
        //if(this.layout == Layout.ROW_LAYOUT) {
        return concat(B, new DenseMatrix32F(rows() + B.rows(), cols()));
        //}
        //else if(this.layout == Layout.COLUMN_LAYOUT) {
        //    Preconditions.checkArgument(rows == B.rows());
        //    return concat(B, new DenseMatrix32(rows, B.cols() + cols));
        //}
        //else {
        //    throw new IllegalStateException("Unknown layout: " + layout.name());
        //}
    }

    @Override
    public Matrix32F concat(final Matrix32F B, final Matrix32F C) {
        //if(this.layout == Layout.ROW_LAYOUT) {
        for (int row = 0; row < C.rows(); row++) {
            for (int col = 0; col < cols(); col++) {
                float val = row < rows() ? this.get(row, col) : B.get(row, col);
                C.set(row, col, val);
            }
        }
        /*}
        else if(this.layout == Layout.COLUMN_LAYOUT) {
            Preconditions.checkArgument(rows == B.rows());
            Preconditions.checkArgument(C.rows() == rows && C.cols() == cols + B.cols());
            for (int row = 0; row < C.rows(); row++) {
                for (int col = 0; col < cols; col++) {
                    float val = col < cols ? this.get(row, col) : B.get(row, col);
                    C.set(row, col, val);
                }
            }
        }
        else {
            throw new IllegalStateException("Unknown layout: " + layout.name());
        }*/
        return C;
    }

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator rowIterator() {
        return new RowIterator(this);
    }

    @Override
    public RowIterator rowIterator(final long startRow, final long endRow) {
        return new RowIterator(this, startRow, endRow);
    }

    // ---------------------------------------------------

    private static final class RowIterator implements Matrix32F.RowIterator {

        private DenseMatrix32F self;

        private final long end;

        private final long start;

        private long currentRow;

        private final long rowsToFetch;

        private long rowsFetched;

        private Random rand;

        // ---------------------------------------------------

        public RowIterator(final Matrix32F m) { this(m, 0, m.rows()); }
        public RowIterator(final Matrix32F m, final long startRow, final long endRow) {
            this.self = (DenseMatrix32F)m;
            Preconditions.checkArgument(startRow >= 0 && startRow < self.rows());
            Preconditions.checkArgument(endRow >= startRow && endRow <= self.rows());
            this.start = startRow;
            this.end   = endRow;
            this.rowsToFetch = endRow - startRow;
            this.rand  = new Random();
            reset();
        }

        // ---------------------------------------------------

        @Override
        public boolean hasNext() {
            return rowsFetched < rowsToFetch;
        }

        @Override
        public void next() {
            // the generic case is just currentRow++, but the reset method only sets rowsFetched = 0
            if(rowsFetched == 0) {
                currentRow = 0;
            }
            else {
                currentRow++;
            }
            rowsFetched++;
            // can overflow if nextRandom and next is called alternatingly
            if(currentRow >= end) {
                currentRow = start;
            }
        }

        @Override
        public void nextRandom() {
            rowsFetched++;
            currentRow = start + rand.nextInt((int)end);
        }

        @Override
        public float value(final long col) {
            return self.data[(int)(currentRow * self.rows() + col)];
        }

        @Override
        public Matrix32F get() {
            return get(0, (int) self.cols());
        }

        @Override
        public Matrix32F get(final long from, final long size) {
            final float v[] = new float[(int)size];
            try {
                System.arraycopy(self.data, (int)(currentRow * self.cols() + from), v, 0, (int)size);
            } catch(ArrayIndexOutOfBoundsException e) {
                System.out.println("failed copy " +
                        "from: " + (int)(currentRow * self.rows() + from) + "; " +
                        "currentRow: " + currentRow + "; " +
                        "from: " + from + ";  " +
                        "length: " + size + "; " +
                        "array.length: " + self.data.length);
                throw e;
            }
            return new DenseMatrix32F(1, size, v);
        }

        @Override
        public void reset() {
            rowsFetched = 0;
        }

        @Override
        public long size() {
            return rowsToFetch;
        }

        @Override
        public long rowNum() { return currentRow; }
    }
}
