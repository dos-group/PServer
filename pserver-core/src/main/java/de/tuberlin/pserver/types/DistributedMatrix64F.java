package de.tuberlin.pserver.types;


import de.tuberlin.pserver.math.matrix.*;
import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.utils.MatrixBuilder;

public class DistributedMatrix64F implements Matrix64F {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final long rows;

    private final long cols;

    private final IMatrixPartitioner partitioner;

    private final PartitionShape shape;

    private final Matrix64F matrixSection;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DistributedMatrix64F(final ProgramContext programContext,
                                final long rows, final long cols,
                                final Layout layout,
                                final Format format,
                                final int[] atNodes,
                                final Class<? extends IMatrixPartitioner> partitionerType) {

        this.rows = rows;

        this.cols = cols;

        this.partitioner = IMatrixPartitioner.newInstance(
                partitionerType, 
                rows, cols, 
                programContext.runtimeContext.nodeID, 
                atNodes
        );
        
        this.shape = partitioner.getPartitionShape();
        
        this.matrixSection = new MatrixBuilder()
                .dimension(shape.rows, shape.cols)
                .format(format)
                .layout(layout)
                .elementType(ElementType.DOUBLE_MATRIX)
                .build();
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

    @Override public long sizeOf() { return rows * cols * Double.BYTES; }

    @Override public Layout layout() { return matrixSection.layout(); }

    @Override public void lock() { matrixSection.lock(); }

    @Override public void unlock() { matrixSection.unlock(); }

    @Override public void setOwner(Object owner) { matrixSection.setOwner(owner); }

    @Override public Object getOwner() { return matrixSection.getOwner(); }

    @Override public Matrix64F copy() { return matrixSection.copy(); }

    @Override public Matrix64F copy(long rows, long cols) { return matrixSection.copy(rows, cols); }

    // ---------------------------------------------------
    // SETTER.
    // ---------------------------------------------------

    @Override
    public void set(final long row, final long col, final Double value) {
        matrixSection.set(partitioner.translateGlobalToLocalRow(row), partitioner.translateGlobalToLocalCol(col), value);
    }

    @Override
    public Matrix64F setDiagonalsToZero() {
        return matrixSection.setDiagonalsToZero();
    }

    @Override
    public Matrix64F setDiagonalsToZero(final Matrix<Double> B) {
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
    public Double get(final long index) {
        final long row = index / rows;
        final long col = index % cols;
        return matrixSection.get(partitioner.translateGlobalToLocalRow(row), partitioner.translateGlobalToLocalCol(col));
    }

    @Override
    public Double get(final long row, final long col) {
        return matrixSection.get(partitioner.translateGlobalToLocalRow(row), partitioner.translateGlobalToLocalCol(col));
    }

    @Override
    public Matrix64F getRow(final long row) {
        return matrixSection.getRow(partitioner.translateGlobalToLocalRow(row));
    }

    @Override
    public Matrix64F getRow(final long row, final long from, final long to) {
        return matrixSection.getRow(
                partitioner.translateGlobalToLocalRow(row),
                partitioner.translateGlobalToLocalCol(from),
                partitioner.translateGlobalToLocalCol(to)
        );
    }

    @Override
    public Matrix64F getCol(final long col) {
        return matrixSection.getCol(partitioner.translateGlobalToLocalCol(col));
    }

    @Override
    public Matrix64F getCol(final long col, final long from, final long to) {
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
    public Matrix64F applyOnElements(final UnaryOperator<Double> f) {
        return matrixSection.applyOnElements(f);
    }

    @Override
    public Matrix64F applyOnElements(final UnaryOperator<Double> f, final Matrix<Double> B) {
        return matrixSection.applyOnElements(f, B);
    }

    @Override
    public Matrix64F applyOnElements(final Matrix<Double> B, final BinaryOperator<Double> f) {
        return matrixSection.applyOnElements(B, f);
    }

    @Override
    public Matrix64F applyOnElements(final Matrix<Double> B, final BinaryOperator<Double> f, final Matrix<Double> C) {
        return matrixSection.applyOnElements(B, f, C);
    }

    @Override
    public Matrix64F applyOnElements(final MatrixElementUnaryOperator<Double> f) {
        return matrixSection.applyOnElements(f);
    }

    @Override
    public Matrix64F applyOnElements(final MatrixElementUnaryOperator<Double> f, final Matrix<Double> B) {
        return matrixSection.applyOnElements(f, B);
    }

    @Override
    public Matrix64F applyOnNonZeroElements(final MatrixElementUnaryOperator<Double> f) {
        return matrixSection.applyOnElements(f);
    }

    @Override
    public Matrix64F applyOnNonZeroElements(final MatrixElementUnaryOperator<Double> f, final Matrix<Double> B) {
        return matrixSection.applyOnElements(f, B);
    }

    // ---------------------------------------------------
    // ASSIGN.
    // ---------------------------------------------------

    @Override
    public Matrix64F assign(final Matrix<Double> m) {
        return matrixSection.assign(m);
    }

    @Override
    public Matrix64F assign(final Double v) {
        return matrixSection.assign(v);
    }

    @Override
    public Matrix64F assignRow(final long row, final Matrix<Double> m) {
        return matrixSection.assignRow(partitioner.translateGlobalToLocalRow(row), m);
    }

    @Override
    public Matrix64F assignColumn(final long col, final Matrix<Double> m) {
        return matrixSection.assignColumn(partitioner.translateGlobalToLocalCol(col), m);
    }

    @Override
    public Matrix64F assign(final long rowOffset, long colOffset, final Matrix<Double> m) {
        return matrixSection.assign(partitioner.translateGlobalToLocalRow(rowOffset), partitioner.translateGlobalToLocalCol(colOffset), m);
    }

    // ---------------------------------------------------
    // AGGREGATION.
    // ---------------------------------------------------

    @Override
    public Double aggregate(final BinaryOperator<Double> combiner, final UnaryOperator<Double> mapper, final Matrix<Double> result) {
        return matrixSection.aggregate(combiner, mapper, result);
    }

    @Override
    public Matrix64F aggregateRows(final MatrixAggregation<Double> f) {
        return matrixSection.aggregateRows(f);
    }

    @Override
    public Matrix64F aggregateRows(final MatrixAggregation<Double> f, final Matrix<Double> result) {
        return matrixSection.aggregateRows(f, result);
    }

    @Override
    public Double sum() {
        return matrixSection.sum();
    }

    // ---------------------------------------------------
    // ARITHMETIC.
    // ---------------------------------------------------

    @Override
    public Matrix64F add(final Matrix<Double> B) {
        return matrixSection.add(B);
    }

    @Override
    public Matrix64F add(final Matrix<Double> B, final Matrix<Double> C) {
        return matrixSection.add(B, C);
    }

    @Override
    public Matrix64F addVectorToRows(final Matrix<Double> v) {
        return matrixSection.addVectorToRows(v);
    }

    @Override
    public Matrix64F addVectorToRows(final Matrix<Double> v, final Matrix<Double> B) {
        return matrixSection.addVectorToRows(v, B);
    }

    @Override
    public Matrix64F addVectorToCols(final Matrix<Double> v) {
        return matrixSection.addVectorToCols(v);
    }

    @Override
    public Matrix64F addVectorToCols(final Matrix<Double> v, final Matrix<Double> B) {
        return matrixSection.addVectorToCols(v, B);
    }

    @Override
    public Matrix64F sub(final Matrix<Double> B) {
        return matrixSection.sub(B);
    }

    @Override
    public Matrix64F sub(final Matrix<Double> B, final Matrix<Double> C) {
        return matrixSection.sub(B, C);
    }

    @Override
    public Matrix64F mul(final Matrix<Double> B) {
        return matrixSection.mul(B);
    }

    @Override
    public Matrix64F mul(final Matrix<Double> B, final Matrix<Double> C) {
        return matrixSection.mul(B, C);
    }

    @Override
    public Matrix64F scale(final Double a) {
        return matrixSection.scale(a);
    }

    @Override
    public Matrix64F scale(final Double a, final Matrix<Double> B) {
        return matrixSection.scale(a, B);
    }

    @Override
    public Matrix64F transpose() {
        return matrixSection.transpose();
    }

    @Override
    public Matrix64F transpose(Matrix<Double> B) {
        return matrixSection.transpose(B);
    }

    @Override
    public Matrix64F invert() {
        return matrixSection.invert();
    }

    @Override
    public Matrix64F invert(final Matrix<Double> B) {
        return matrixSection.invert(B);
    }

    @Override
    public Double norm(final int p) {
        return matrixSection.norm(p);
    }

    @Override
    public Double dot(final Matrix<Double> B) {
        return matrixSection.dot(B);
    }

    // ---------------------------------------------------
    // SLICING.
    // ---------------------------------------------------

    @Override
    public Matrix64F subMatrix(final long rowOffset, final long colOffset, final long rows, final long cols) {
        return matrixSection.subMatrix(partitioner.translateGlobalToLocalRow(rowOffset), partitioner.translateGlobalToLocalCol(colOffset), rows, cols);
    }

    @Override
    public Matrix64F concat(final Matrix<Double> B) {
        return matrixSection.concat(B);
    }

    @Override
    public Matrix64F concat(final Matrix<Double> B, final Matrix<Double> C) {
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

    private Matrix64F.RowIterator localRowIterator(final Matrix64F.RowIterator it) {
        return new Matrix64F.RowIterator() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public void next() { it.next(); }
            @Override public void nextRandom() { it.nextRandom(); }
            @Override public Double value(long col) { return it.value(col); }
            @Override public Matrix64F get() { return it.get(); }
            @Override public Matrix64F get(long from, long size) { return it.get(from, size); }
            @Override public void reset() { it.reset(); }
            @Override public long size() { return it.size(); }
            @Override public long rowNum() { return (int) partitioner.translateLocalToGlobalRow(it.rowNum()); }
        };
    }
}
