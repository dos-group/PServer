package de.tuberlin.pserver.math.matrix.sparse;

import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.dense.DVector;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SMutableMatrix extends AbstractMatrix {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public final class MtxPos implements Serializable {

        public final long row;

        public final long col;


        public MtxPos(final long row, final long col) {
            this.row = row;
            this.col = col;
        }

        @Override
        public int hashCode() {
        //    return new HashCodeBuilder().append(row).append(col).build();
            return (int) (row * cols + col);
        }

        @Override
        public boolean equals(final Object o) {
            if (o instanceof MtxPos) {
                MtxPos that = (MtxPos) o;
                return (this.row == that.row && this.col == that.col);
            }
            return false;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Map<MtxPos, Double> data;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SMutableMatrix(final long rows, final long cols, final Layout layout) {
        super(rows, cols, layout);
        this.data = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public double get(long row, long col) {
        final Double value = data.get(new MtxPos(row, col));
        return (value == null) ? 0 : value;
    }

    @Override
    public void set(long row, long col, double value) {
        if(value == 0.0) {
            data.remove(new MtxPos(row, col));
        } else {
            data.put(new MtxPos(row, col), value);
        }
    }

    @Override
    public double atomicGet(long row, long col) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public void atomicSet(long row, long col, double value) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public double[] toArray() {
        return new double[0];
    }

    @Override
    public void setArray(double[] data) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public RowIterator rowIterator() {
        return new SMatrix2RowIterator(this);
    }

    @Override
    public RowIterator rowIterator(int startRow, int endRow) {
        return new SMatrix2RowIterator(this, startRow, endRow);
    }

    @Override
    protected Matrix newInstance(long rows, long cols) {
        return new SMutableMatrix(rows, cols, Layout.ROW_LAYOUT);
    }

    @Override
    public Matrix transpose(Matrix A) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Vector rowAsVector() {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Vector rowAsVector(long row) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Vector rowAsVector(long row, long from, long to) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Vector colAsVector() {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Vector colAsVector(long col) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Vector colAsVector(long col, long from, long to) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix assign(Matrix v) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix assign(double v) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix assignRow(long row, Vector v) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix assignColumn(long col, Vector v) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix copy() {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix subMatrix(long row, long col, long rowSize, long colSize) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix assign(long row, long col, Matrix m) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix applyOnNonZeroElements(MatrixElementUnaryOperator f, Matrix B) {
        for (Map.Entry<MtxPos, Double> ele : data.entrySet()) {
            long row = ele.getKey().row;
            long col = ele.getKey().col;
            B.set(row, col, f.apply(row, col, ele.getValue()));
        }
        return this;
    }

    public static class SMatrix2RowIterator extends AbstractRowIterator {

        public SMatrix2RowIterator(AbstractMatrix mat) {
            super(mat);
        }

        public SMatrix2RowIterator(AbstractMatrix mat, int startRow, int endRow) {
            super(mat, startRow, endRow);
        }

        @Override
        public Vector getAsVector() {
            return getAsVector(0, Utils.toInt(numCols()));
        }

        @Override
        public Vector getAsVector(int from, int size) {
            System.out.println("curRow:" + currentRow);
            if(from < 0 || size > numCols()) {
                throw new IllegalArgumentException();
            }
            DVector rowVec = new DVector(size, Layout.ROW_LAYOUT);
            for(long col = from; col < size; col++) {
                double val = target.get(currentRow, col);
                rowVec.set(col, val);
            }
            return rowVec;
        }
    }
}
