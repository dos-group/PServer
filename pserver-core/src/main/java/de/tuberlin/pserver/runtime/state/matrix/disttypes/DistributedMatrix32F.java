package de.tuberlin.pserver.runtime.state.matrix.disttypes;


import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.state.matrix.MatrixBuilder;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.MatrixPartitioner;

public class DistributedMatrix32F implements Matrix32F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private long rows;

    private long cols;

    private MatrixPartitioner partitioner;

    private PartitionShape shape;

    private Matrix32F matrixSection;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DistributedMatrix32F() { this(null, -1, -1, null, null, null); }
    public DistributedMatrix32F(final ProgramContext programContext,
                                final long rows, final long cols,
                                final MatrixFormat matrixFormat,
                                final int[] atNodes,
                                final Class<? extends MatrixPartitioner> partitionerType) {

        if (programContext != null) {

            this.rows = rows;
            this.cols = cols;
            this.partitioner = MatrixPartitioner.newInstance(
                    partitionerType,
                    rows, cols,
                    programContext.nodeID,
                    atNodes
            );

            this.shape = partitioner.getPartitionShape();
            this.matrixSection = new MatrixBuilder()
                    .dimension(shape.rows, shape.cols)
                    .matrixFormat(matrixFormat)
                    .elementType(ElementType.FLOAT_MATRIX)
                    .build();
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long partitionBaseRowOffset() { return shape.rowOffset; }

    public long partitionBaseColOffset() { return shape.colOffset; }

    public long partitionNumRows() { return shape.rows; }

    public long partitionNumCols() { return shape.cols; }

    public PartitionShape getPartitionShape() { return shape; }

    // ---------------------------------------------------

    @Override public long rows() { return rows; }

    @Override public long cols() { return cols; }

    @Override public long sizeOf() { return rows * cols * Float.BYTES; }

    @Override public void lock() { matrixSection.lock(); }

    @Override public void unlock() { matrixSection.unlock(); }

    @Override public void setOwner(Object owner) { matrixSection.setOwner(owner); }

    @Override public Object getOwner() { return matrixSection.getOwner(); }

    @Override public Matrix32F copy() { return matrixSection.copy(); }

    @Override public Matrix32F copy(long rows, long cols) { return matrixSection.copy(rows, cols); }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(final long row, final long col, final Float value) {
        matrixSection.set(partitioner.translateGlobalToLocalRow(row), partitioner.translateGlobalToLocalCol(col), value);
    }

    @Override
    public Matrix32F setDiagonalsToZero() {
        return matrixSection.setDiagonalsToZero();
    }

    @Override
    public Matrix32F setDiagonalsToZero(final Matrix<Float> B) {
        return matrixSection.setDiagonalsToZero(B);
    }

    @Override
    public void setArray(final Object data) {
        matrixSection.setArray(data);
    }

    // ---------------------------------------------------
    // GETTER.
    // ---------------------------------------------------

    @Override
    public Float get(final long index) {
        final long row = index / rows;
        final long col = index % cols;
        return matrixSection.get(partitioner.translateGlobalToLocalRow(row), partitioner.translateGlobalToLocalCol(col));
    }

    @Override
    public Float get(final long row, final long col) {
        return matrixSection.get(partitioner.translateGlobalToLocalRow(row), partitioner.translateGlobalToLocalCol(col));
    }

    @Override
    public Matrix32F getRow(final long row) {
        return matrixSection.getRow(partitioner.translateGlobalToLocalRow(row));
    }

    @Override
    public Matrix32F getRow(final long row, final long from, final long to) {
        return matrixSection.getRow(
                partitioner.translateGlobalToLocalRow(row),
                partitioner.translateGlobalToLocalCol(from),
                partitioner.translateGlobalToLocalCol(to)
        );
    }

    @Override
    public Matrix32F getCol(final long col) {
        return matrixSection.getCol(partitioner.translateGlobalToLocalCol(col));
    }

    @Override
    public Matrix32F getCol(final long col, final long from, final long to) {
        return matrixSection.getCol(
                partitioner.translateGlobalToLocalCol(col),
                partitioner.translateGlobalToLocalRow(from),
                partitioner.translateGlobalToLocalRow(to)
        );
    }

    @Override
    public Object toArray() {
        return matrixSection.toArray();
    }

    // ---------------------------------------------------
    // APPLY ON ELEMENTS.
    // ---------------------------------------------------

    @Override
    public Matrix32F applyOnElements(final UnaryOperator<Float> f) {
        return matrixSection.applyOnElements(f);
    }

    @Override
    public Matrix32F applyOnElements(final UnaryOperator<Float> f, final Matrix<Float> B) {
        return matrixSection.applyOnElements(f, B);
    }

    @Override
    public Matrix32F applyOnElements(final Matrix<Float> B, final BinaryOperator<Float> f) {
        return matrixSection.applyOnElements(B, f);
    }

    @Override
    public Matrix32F applyOnElements(final Matrix<Float> B, final BinaryOperator<Float> f, final Matrix<Float> C) {
        return matrixSection.applyOnElements(B, f, C);
    }

    @Override
    public Matrix32F applyOnElements(final MatrixElementUnaryOperator<Float> f) {
        return matrixSection.applyOnElements(f);
    }

    @Override
    public Matrix32F applyOnElements(final MatrixElementUnaryOperator<Float> f, final Matrix<Float> B) {
        return matrixSection.applyOnElements(f, B);
    }

    @Override
    public Matrix32F applyOnNonZeroElements(final MatrixElementUnaryOperator<Float> f) {
        return matrixSection.applyOnElements(f);
    }

    @Override
    public Matrix32F applyOnNonZeroElements(final MatrixElementUnaryOperator<Float> f, final Matrix<Float> B) {
        return matrixSection.applyOnElements(f, B);
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix32F assign(final Matrix<Float> m) {
        return matrixSection.assign(m);
    }

    @Override
    public Matrix32F assign(final Float v) {
        return matrixSection.assign(v);
    }

    @Override
    public Matrix32F assignRow(final long row, final Matrix<Float> m) {
        return matrixSection.assignRow(partitioner.translateGlobalToLocalRow(row), m);
    }

    @Override
    public Matrix32F assignColumn(final long col, final Matrix<Float> m) {
        return matrixSection.assignColumn(partitioner.translateGlobalToLocalCol(col), m);
    }

    @Override
    public Matrix32F assign(final long rowOffset, long colOffset, final Matrix<Float> m) {
        return matrixSection.assign(partitioner.translateGlobalToLocalRow(rowOffset), partitioner.translateGlobalToLocalCol(colOffset), m);
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public Float aggregate(final BinaryOperator<Float> combiner, final UnaryOperator<Float> mapper, final Matrix<Float> result) {
        return matrixSection.aggregate(combiner, mapper, result);
    }

    @Override
    public Matrix32F aggregateRows(final MatrixAggregation<Float> f) {
        return matrixSection.aggregateRows(f);
    }

    @Override
    public Matrix32F aggregateRows(final MatrixAggregation<Float> f, final Matrix<Float> result) {
        return matrixSection.aggregateRows(f, result);
    }

    @Override
    public Float sum() {
        return matrixSection.sum();
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix32F add(final Matrix<Float> B) {
        return matrixSection.add(B);
    }

    @Override
    public Matrix32F add(final Matrix<Float> B, final Matrix<Float> C) {
        return matrixSection.add(B, C);
    }

    @Override
    public Matrix32F addVectorToRows(final Matrix<Float> v) {
        return matrixSection.addVectorToRows(v);
    }

    @Override
    public Matrix32F addVectorToRows(final Matrix<Float> v, final Matrix<Float> B) {
        return matrixSection.addVectorToRows(v, B);
    }

    @Override
    public Matrix32F addVectorToCols(final Matrix<Float> v) {
        return matrixSection.addVectorToCols(v);
    }

    @Override
    public Matrix32F addVectorToCols(final Matrix<Float> v, final Matrix<Float> B) {
        return matrixSection.addVectorToCols(v, B);
    }

    @Override
    public Matrix32F sub(final Matrix<Float> B) {
        return matrixSection.sub(B);
    }

    @Override
    public Matrix32F sub(final Matrix<Float> B, final Matrix<Float> C) {
        return matrixSection.sub(B, C);
    }

    @Override
    public Matrix32F mul(final Matrix<Float> B) {
        return matrixSection.mul(B);
    }

    @Override
    public Matrix32F mul(final Matrix<Float> B, final Matrix<Float> C) {
        return matrixSection.mul(B, C);
    }

    @Override
    public Matrix32F scale(final Float a) {
        return matrixSection.scale(a);
    }

    @Override
    public Matrix32F scale(final Float a, final Matrix<Float> B) {
        return matrixSection.scale(a, B);
    }

    @Override
    public Matrix32F transpose() {
        return matrixSection.transpose();
    }

    @Override
    public Matrix32F transpose(Matrix<Float> B) {
        return matrixSection.transpose(B);
    }

    @Override
    public Matrix32F invert() {
        return matrixSection.invert();
    }

    @Override
    public Matrix32F invert(final Matrix<Float> B) {
        return matrixSection.invert(B);
    }

    @Override
    public Float norm(final int p) {
        return matrixSection.norm(p);
    }

    @Override
    public Float dot(final Matrix<Float> B) {
        return matrixSection.dot(B);
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix32F subMatrix(final long rowOffset, final long colOffset, final long rows, final long cols) {
        return matrixSection.subMatrix(partitioner.translateGlobalToLocalRow(rowOffset), partitioner.translateGlobalToLocalCol(colOffset), rows, cols);
    }

    @Override
    public Matrix32F concat(final Matrix<Float> B) {
        return matrixSection.concat(B);
    }

    @Override
    public Matrix32F concat(final Matrix<Float> B, final Matrix<Float> C) {
        return matrixSection.concat(B, C);
    }

    // ---------------------------------------------------
    // ROW ITERATOR.
    // ---------------------------------------------------

    @Override
    public RowIterator rowIterator() {
        return localRowIterator(matrixSection.rowIterator());
    }

    @Override
    public RowIterator rowIterator(final long startRow, final long endRow) {
        return localRowIterator(
                matrixSection.rowIterator(
                        partitioner.translateGlobalToLocalRow(startRow),
                        partitioner.translateGlobalToLocalRow(endRow)
                )
        );
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private RowIterator localRowIterator(final RowIterator it) {
        return new RowIterator() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public void next() { it.next(); }
            @Override public void nextRandom() { it.nextRandom(); }
            @Override public Float value(long col) { return it.value(col); }
            @Override public Matrix32F get() { return it.get(); }
            @Override public Matrix32F get(long from, long size) { return it.get(from, size); }
            @Override public void reset() { it.reset(); }
            @Override public long size() { return it.size(); }
            @Override public long rowNum() { return (int) partitioner.translateLocalToGlobalRow(it.rowNum()); }
        };
    }


    public String toString() {
        return matrixSection.toString();
    }
}
