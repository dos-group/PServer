package de.tuberlin.pserver.matrix.crdt;


import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.tuples.Tuple3;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Ideas:
 *
 * 1. Exact solution.
 * 2. Weight every new input by 1/numOfReplicas (and wait until the value is within a certain range)
 * 3. Exact solution but without session (just always include the newest updates)
 * 4.
 */

public class NoSessionAvgDenseMatrix32F extends AbstractCRDTMatrix implements AvgMatrix32F {
    private final long rows;
    private final long cols;
    private DenseMatrix32F matrix;
    private final DenseMatrix32F counts;
    private final int noOfReplicas;
    private final Map<Tuple3<Long, Long, Double>, List<MatrixAvgOperation<Double>>> queues;
    private volatile boolean sessionFinished;
    //private final Object session;

    public NoSessionAvgDenseMatrix32F(long rows, long cols, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.matrix = new DenseMatrix32F(rows, cols);
        this.counts = new DenseMatrix32F(rows, cols);
        // Is a hashmap the best solution, what about collisions etc?
        this.queues = new HashMap<>();
        this.sessionFinished = true;
        this.noOfReplicas = noOfReplicas;
        //this.session = new Object();
        this.rows = rows;
        this.cols = cols;
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        MatrixAvgOperation<Float> mop = (MatrixAvgOperation<Float>) op;

        switch (mop.getOpType()) {
            case AVERAGE: return remoteIncludeInAvg(mop);
            default:      return false;
        }
    }

    private boolean remoteIncludeInAvg(MatrixAvgOperation<Float> mop) {
        return makeAverage(mop.getRow(), mop.getCol(), mop.getValue());
    }

    private boolean makeAverage(long row, long col, Float value) {
        Float count = counts.get(row, col);
        Float avg = matrix.get(row, col);
        /*System.out.println("Average: " + avg);
        System.out.println("New value: " + value);*/

        avg = (avg * count + value) / ++count;

        //System.out.println("New average: " + avg);
        //System.out.println();

        counts.set(row, col, count);
        matrix.set(row, col, avg);

        // If all updates are received a new session starts
       /* if(counts.get(row, col) == noOfReplicas) {
            sessionFinished = true;
            //sessionIDs.set(row, col, sessionIDs.get(row, col) + 1);
        }*/

        return true;
    }

    public synchronized void includeInAvg(long row, long col, Float value) {
        // Wait for previous session to finish
       /* while(!sessionFinished) {
            try {
                session.wait(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }*/

        // New session
        matrix.set(row, col, 0f);
        counts.set(row, col, 0f);

        // New average
        makeAverage(row, col, value);

        broadcast(new MatrixAvgOperation<>(value, row, col, MatrixAvgOperation.MatrixOpType.AVERAGE, 0));
    }

    protected DenseMatrix32F newInstance(long rows, long cols) {
        return new DenseMatrix32F(rows, cols);
    }

    @Override
    public Matrix32F copy() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F copy(long rows, long cols) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void set(long row, long col, Float value) {
        // TODO: this should not be a supported method in production, only for tests
        matrix.set(row, col, value);
        //throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F setDiagonalsToZero() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F setDiagonalsToZero(Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void setArray(Object data) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Float get(long index) {
        // TODO: is this correct? (I think it is)
        return matrix.get((index / cols), index % cols);
    }

    @Override
    public Float get(long row, long col) {
        return matrix.get(row, col);
    }

    @Override
    public Matrix32F getRow(long row) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F getRow(long row, long from, long to) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F getCol(long col) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F getCol(long col, long from, long to) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Object toArray() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f, Matrix<Float> B) {
        if(!this.equals(B)) throw new IllegalArgumentException("CRDTs must apply operations on themselves.");

        for (int i = 0; i < B.rows(); ++i) {
            for (int j = 0; j < B.cols(); ++j) {
                includeInAvg(i, j, f.apply(this.get(i, j)));
            }
        }
        return this;
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f, Matrix<Float> C) {
        if(!this.equals(C)) throw new IllegalArgumentException("CRDTs must apply operations on themselves.");
        Utils.checkShapeEqual(this, B);

        for (int i = 0; i < rows(); ++i) {
            for (int j = 0; j < cols(); ++j) {
                this.includeInAvg(i, j, f.apply(this.get(i, j), B.get(i, j)));
            }
        }
        return this;
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F assign(Matrix<Float> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F assign(Float aFloat) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F assignRow(long row, Matrix<Float> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F assignColumn(long col, Matrix<Float> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F assign(long rowOffset, long colOffset, Matrix<Float> m) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Float aggregate(BinaryOperator<Float> combiner, UnaryOperator<Float> mapper, Matrix<Float> result) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f, Matrix<Float> result) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Float sum() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F add(Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F add(Matrix<Float> B, Matrix<Float> C) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v, Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v, Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public synchronized Matrix32F sub(Matrix<Float> B) {
        matrix.sub(B, matrix);
        return matrix;
    }

    @Override
    public Matrix32F sub(Matrix<Float> B, Matrix<Float> C) {
        if(!this.equals(C)) throw new IllegalArgumentException("CRDTs must apply operations on themselves.");
        Utils.checkShapeEqual(this, B);
        return this.applyOnElements(B, (x, y) -> x - y, this);
    }

    @Override
    public Matrix32F mul(Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F mul(Matrix<Float> B, Matrix<Float> C) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F scale(Float a) {
        return scale(a, this);
    }

    @Override
    public Matrix32F scale(Float a, Matrix<Float> B) {
        if(!this.equals(B)) throw new IllegalArgumentException("CRDTs must apply operations on themselves.");
        Utils.checkShapeEqual(this, B);
        return applyOnElements(x -> a * x, this);
    }

    @Override
    public Matrix32F transpose() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F transpose(Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F invert() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F invert(Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Float norm(int p) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Float dot(Matrix<Float> B) {
        float result = 0;
        for (int i = 0; i < cols * rows; i++) {
            result += this.get(i) * B.get(i);
        }
        return result;
    }

    @Override
    public Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F concat(Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix32F concat(Matrix<Float> B, Matrix<Float> C) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public RowIterator rowIterator() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public RowIterator rowIterator(long startRow, long endRow) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public long rows() {
        return matrix.rows();
    }

    @Override
    public long cols() {
        return matrix.cols();
    }

    @Override
    public long sizeOf() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void lock() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void unlock() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void setOwner(Object owner) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Object getOwner() {
        throw new UnsupportedOperationException("Operation not supported");
    }
}
