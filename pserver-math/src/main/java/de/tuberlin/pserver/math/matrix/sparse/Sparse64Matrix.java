package de.tuberlin.pserver.math.matrix.sparse;

import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.Utils;
import gnu.trove.map.hash.TIntDoubleHashMap;
import org.apache.commons.lang3.NotImplementedException;


public class Sparse64Matrix extends AbstractMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final TIntDoubleHashMap data = new TIntDoubleHashMap();

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Sparse64Matrix(final long rows, final long cols, final Layout layout) {
        super(rows, cols, layout);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public double get(long index) {
        final Double value = data.get(Utils.toInt(index));
        return (value == null) ? 0. : value;
    }

    @Override
    public double get(long row, long col) {
        final Double value = data.get(Utils.getPos(row, col, this));
        return (value == null) ? 0. : value;
    }

    @Override
    public void set(long row, long col, double value) {
        data.put(Utils.getPos(row, col, this), value);
    }

    @Override
    public double[] toArray() {
        throw new NotImplementedException("not impl");
    }

    @Override
    public void setArray(double[] data) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public RowIterator rowIterator() {
        throw new NotImplementedException("not impl");
    }

    @Override
    public RowIterator rowIterator(int startRow, int endRow) {
        throw new NotImplementedException("not impl");
    }

    @Override
    protected Matrix newInstance(long rows, long cols) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix transpose(Matrix A) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix getRow(long row) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix getRow(long row, long from, long to) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix getCol(long col) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix getCol(long col, long from, long to) {
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
    public Matrix assignRow(long row, Matrix v) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix assignColumn(long col, Matrix v) {
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
    public Matrix assign(long rowOffset, long colOffset, Matrix m) {
        throw new NotImplementedException("not impl");
    }

    @Override
    public Matrix applyOnNonZeroElements(MatrixElementUnaryOperator f, Matrix B) {
        throw new NotImplementedException("not impl");
    }
}
