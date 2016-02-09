package de.tuberlin.pserver.math.matrix32.impl;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix32.Matrix32;
import de.tuberlin.pserver.math.matrix32.Matrix32MetaData;
import de.tuberlin.pserver.math.matrix32.operations.BinaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixAggregation32;
import de.tuberlin.pserver.math.matrix32.operations.MatrixElementUnaryOperator32;
import de.tuberlin.pserver.math.matrix32.operations.UnaryOperator32;
import de.tuberlin.pserver.math.matrix32.partitioner.PartitionerType;

import java.util.Arrays;

public class DenseMatrix32 extends Matrix32MetaData implements Matrix32 {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public float[] data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DenseMatrix32(DenseMatrix32 toCopy) {
        super(toCopy.partitioner.getPartitionerType(), toCopy.nodeID, toCopy.nodes, toCopy.globalRows, toCopy.globalCols);
        this.data = (data == null) ? new float[(int)(rows() * cols())] : Preconditions.checkNotNull(data);
    }

    public DenseMatrix32(long globalRows, long globalCols) {
        this(PartitionerType.NO_PARTITIONER, -1, null, globalRows, globalCols, null);
    }

    public DenseMatrix32(long globalRows, long globalCols, final float[] data) {
        this(PartitionerType.NO_PARTITIONER, -1, null, globalRows, globalCols, data);
    }

    public DenseMatrix32(PartitionerType type, int nodeID, int[] nodes, long globalRows, long globalCols, final float[] data) {
        super(type, nodeID, nodes, globalRows, globalCols);
        this.data = (data == null) ? new float[(int)(rows() * cols())] : Preconditions.checkNotNull(data);
    }

    // ---------------------------------------------------
    // MetaData Methods.
    // ---------------------------------------------------

    @Override
    public Object toArray() {
        return data;
    }

    @Override
    public void setArray(final Object data) {
        float[] mData = (float[])data;
        this.data = mData;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public Matrix32 copy() {
        return new DenseMatrix32(this);
    }

    @Override
    public Matrix32 copy(final long rows, final long cols) {
        Matrix32 result = new DenseMatrix32(rows, cols);
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
    public Matrix32 setDiagonalsToZero() {
        return setDiagonalsToZero(this.copy());
    }

    @Override
    public Matrix32 setDiagonalsToZero(final Matrix32 B) {
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
    public Matrix32 getRow(final long row) {
        return getRow(row, 0, cols());
    }

    @Override
    public Matrix32 getRow(final long row, final long from, final long to) {
        // TODO: Optimize with respect to the layout with array copy.
        Matrix32 r = new DenseMatrix32(1, to - from);
        for (long col = from; col < to; ++col)
            r.set(0, col, data[(int)(row * cols() + col)]);
        return r;
    }

    @Override
    public Matrix32 getCol(final long col) {
        return getCol(col, 0, rows());
    }

    @Override
    public Matrix32 getCol(final long col, final long from, final long to) {
        float[] result = new float[(int)(to - from)];
        for (int i = 0; i < result.length; i++) {
            int row = (int)from + i;
            result[i] = data[(int)(row * cols() + col)];
        }
        return new DenseMatrix32(result.length, 1, result);
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix32 applyOnElements(final UnaryOperator32 f) {
        return applyOnElements(f, new DenseMatrix32(rows(), cols()));
    }

    @Override
    public Matrix32 applyOnElements(final UnaryOperator32 f, final Matrix32 B) {
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                B.set(i, j, f.apply(this.get(i, j)));
            }
        }
        return B;
    }

    @Override
    public Matrix32 applyOnElements(final Matrix32 B, final BinaryOperator32 f) {
        return applyOnElements(B, f, new DenseMatrix32(rows(), cols()));
    }

    @Override
    public Matrix32 applyOnElements(final Matrix32 B, final BinaryOperator32 f, final Matrix32 C) {
        for (int i = 0; i < rows(); ++i) {
            for (int j = 0; j < cols(); ++j) {
                C.set(i, j, f.apply(this.get(i, j), B.get(i, j)));
            }
        }
        return C;
    }

    @Override
    public Matrix32 applyOnElements(final MatrixElementUnaryOperator32 f) {
        return applyOnElements(f, new DenseMatrix32(rows(), cols()));
    }

    @Override
    public Matrix32 applyOnElements(final MatrixElementUnaryOperator32 f, final Matrix32 B) {
        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                B.set(i, j, f.apply(i, j, this.get(i, j)));
            }
        }
        return B;
    }

    @Override
    public Matrix32 applyOnNonZeroElements(final MatrixElementUnaryOperator32 f) {
        return applyOnNonZeroElements(f, new DenseMatrix32(rows(), cols()));
    }

    @Override
    public Matrix32 applyOnNonZeroElements(final MatrixElementUnaryOperator32 f, final Matrix32 B) {
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
    public Matrix32 assign(final Matrix32 v) {
        System.arraycopy(((DenseMatrix32)v).data, 0, data, 0, (int)(rows() * cols()));
        return this;
    }

    @Override
    public Matrix32 assign(final float v) {
        Arrays.fill(data, v);
        return this;
    }

    @Override
    public Matrix32 assignRow(final long row, final Matrix32 v) {
        for (int col = 0; col < cols(); ++col)
            data[(int)(row * cols() + col)] = v.get(col);
        return this;
    }

    @Override
    public Matrix32 assignColumn(final long col, final Matrix32 v) {
        float[] vData = ((DenseMatrix32)v).data;
        for (int row = 0; row < rows(); row++) {
            data[(int)(row * cols() + col)] = vData[row];
        }
        //}
        return this;
    }

    @Override
    public Matrix32 assign(final long rowOffset, final long colOffset, final Matrix32 m) {
        //if (layout == Layout.ROW_LAYOUT && m.layout() == layout && cols == m.cols()) {
        System.arraycopy(((DenseMatrix32)m).data, 0, data, (int) (rowOffset * cols() + colOffset), ((DenseMatrix32)m).data.length);
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
    public float aggregate(final BinaryOperator32 combiner, final UnaryOperator32 mapper, final Matrix32 result) {
        // return aggregateRows(v -> v.aggregate(combiner, mapper), this).aggregate(combiner, Functions.IDENTITY);
        return 0f;
    }

    @Override
    public Matrix32 aggregateRows(final MatrixAggregation32 f) {
        return aggregateRows(f, new DenseMatrix32(rows(), 1));
    }

    @Override
    public Matrix32 aggregateRows(final MatrixAggregation32 f, final Matrix32 result) {
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
    public Matrix32 add(final Matrix32 B) {
        return add(B, new DenseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 add(final Matrix32 B, final Matrix32 C) {
        //Utils.checkShapeEqual(this, B, C);
        //return this.applyOnElements(B, (x, y) -> x + y, C);
        if (B.getClass() == Matrix32.class && C.getClass() == Matrix32.class) {
            DenseMatrix32 b = (DenseMatrix32) B;
            DenseMatrix32 c = (DenseMatrix32) C;
            for (int i = 0; i < data.length; ++i) {
                c.data[i] = data[i] + b.data[i];
            }
        }
        if (B.getClass() == SparseMatrix32.class && C.getClass() == DenseMatrix32.class) {
            SparseMatrix32 b = (SparseMatrix32) B;
            DenseMatrix32 c = (DenseMatrix32) C;
            b.data.forEachEntry((k,v) -> {
                c.data[(int)k] = data[(int)k] + v;
                return true;
            });
            //applyOnElements(B, (x, y) -> x + y, C);
        }
        return this;
    }

    @Override
    public Matrix32 addVectorToRows(final Matrix32 v) {
        return addVectorToRows(v, new DenseMatrix32(rows(), cols()));
    }

    @Override
    public Matrix32 addVectorToRows(final Matrix32 v, final Matrix32 B) {
        for (int row = 0; row < rows(); row++) {
            for (int col = 0; col < cols(); col++) {
                B.set(row, col, this.get(row, col) + v.get(col));
            }
        }
        return B;
    }

    @Override
    public Matrix32 addVectorToCols(final Matrix32 v) {
        return addVectorToCols(v, new DenseMatrix32(rows(), cols()));
    }

    @Override
    public Matrix32 addVectorToCols(final Matrix32 v, final Matrix32 B) {
        for (int col = 0; col < cols(); col++) {
            for (int row = 0; row < rows(); row++) {
                B.set(row, col, this.get(row, col) + v.get(row));
            }
        }
        return B;
    }

    // ----------------------------------------

    @Override
    public Matrix32 sub(final Matrix32 B) {
        return sub(B, new DenseMatrix32(this.rows(), this.cols()));
    }

    @Override
    public Matrix32 sub(final Matrix32 B, final Matrix32 C) {
        if (B.getClass() == DenseMatrix32.class && C.getClass() == DenseMatrix32.class) {
            DenseMatrix32 b = (DenseMatrix32) B;
            DenseMatrix32 c = (DenseMatrix32) C;
            for (int i = 0; i < data.length; ++i) {
                c.data[i] = data[i] - b.data[i];
            }
        }
        if (B.getClass() == SparseMatrix32.class && C.getClass() == DenseMatrix32.class) {
            SparseMatrix32 b = (SparseMatrix32) B;
            DenseMatrix32 c = (DenseMatrix32) C;
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
    public Matrix32 mul(final Matrix32 B) {
        return mul(B, new DenseMatrix32(this.rows(), B.cols()));
    }

    @Override
    public Matrix32 mul(final Matrix32 B, final Matrix32 C) {
        for (int row = 0; row < C.rows(); row++) {
            for (int col = 0; col < C.cols(); col++) {
                C.set(row, col, this.getRow(row).dot(B.getCol(col)));
            }
        }
        return C;
    }

    // ----------------------------------------

    @Override
    public Matrix32 scale(final float a) {
        return scale(a, new DenseMatrix32(rows(), cols()));
    }

    @Override
    public Matrix32 scale(final float a, final Matrix32 B) {
        for (int i = 0; i < data.length; ++i)
            data[i] = data[i] * a;
        return this;
    }

    // ----------------------------------------

    @Override
    public Matrix32 transpose() {
        return transpose(new DenseMatrix32(cols(), rows()));
    }

    @Override
    public Matrix32 transpose(final Matrix32 B) {
        for (int row = 0; row < rows(); row++) {
            for (int col = 0; col < cols(); col++) {
                B.set(col, row, this.get(row, col));
            }
        }
        return B;
    }

    // ----------------------------------------

    @Override
    public Matrix32 invert() {
        return invert(new DenseMatrix32(cols(), rows()));
    }

    @Override
    public Matrix32 invert(final Matrix32 B) {
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
    public float dot(final Matrix32 B) {
        float result = 0;
        //if(this.layout == Layout.ROW_LAYOUT) {
        //Preconditions.checkArgument(rows == 1);
        //Preconditions.checkArgument(B.layout() == Layout.ROW_LAYOUT);
        //Preconditions.checkArgument(B.rows() == 1);
        //Preconditions.checkArgument(cols == B.cols());

        if (B.getClass() == Matrix32.class) {
            DenseMatrix32 b = (DenseMatrix32) B;
            for (int i = 0; i < cols() * rows(); i++) {
                result += this.data[i] * b.data[i];
            }
        }
        if (B.getClass() == SparseMatrix32.class) {
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
    public Matrix32 subMatrix(final long rowOffset, final long colOffset, final long rows, final long cols) {
        //if (layout == Layout.ROW_LAYOUT) {
        final int length = (int)(rows * cols);
        final float[] subData = new float[length];
        try {
            System.arraycopy(data, (int) (rowOffset * cols + colOffset), subData, 0, length);
        }
        catch(ArrayIndexOutOfBoundsException e) {
            throw new RuntimeException(String.format("subMatrix(%d, %d, %d, %d) caused ArrayIndexOfBoundsException", rowOffset, colOffset, rows, cols), e);
        }
        return new DenseMatrix32(rows, cols, subData);
        //} else
        //    throw new UnsupportedOperationException();
    }

    @Override
    public Matrix32 concat(final Matrix32 B) {
        //if(this.layout == Layout.ROW_LAYOUT) {
        return concat(B, new DenseMatrix32(rows() + B.rows(), cols()));
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
    public Matrix32 concat(final Matrix32 B, final Matrix32 C) {
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

    /*@Override
    public RowIterator rowIterator() {
        return new RowIterator(this);
    }

    @Override
    public RowIterator rowIterator(final long startRow, final long endRow) {
        return new RowIterator(this, startRow, endRow);
    }

    // ---------------------------------------------------

    private static final class RowIterator implements Matrix32.RowIterator {

        private Matrix32 self;

        private final long end;

        private final long start;

        private long currentRow;

        private final long rowsToFetch;

        private long rowsFetched;

        private Random rand;

        // ---------------------------------------------------

        public RowIterator(final Matrix32 v) { this(v, 0, (int)Preconditions.checkNotNull(v).rows()); }
        public RowIterator(final Matrix32 v, final long startRow, final long endRow) {
            this.self = v;
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
        public Matrix32 get() {
            return get(0, (int) self.cols);
        }

        @Override
        public Matrix32 get(final long from, final long size) {
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
            return new Matrix32(1, size, v);
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
    }*/
}
