package de.tuberlin.pserver.matrix.crdt;


import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.tuples.Tuple3;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.*;

/**
 * Ideas:
 *
 * 1. Exact solution.
 * 2. Weight every new input by 1/numOfReplicas (and wait until the value is within a certain range)
 * 3. Exact solution but without session (just always include the newest updates)
 * 4.
 */

public class ExactAvgDenseMatrix64F extends AbstractCRDTMatrix implements Matrix64F, AvgMatrix64F {
    private final long rows;
    private final long cols;
    private DenseMatrix64F matrix;
    private final DenseMatrix64F counts;
    private final DenseMatrix64F sessionIDs;
    private final int noOfReplicas;
    private final Map<Tuple3<Long, Long, Double>, List<MatrixAvgOperation<Double>>> queues;
    private volatile boolean sessionFinished;
    private final Object session;

    public ExactAvgDenseMatrix64F(long rows, long cols, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.matrix = new DenseMatrix64F(rows, cols);
        this.sessionIDs = new DenseMatrix64F(rows, cols);
        this.counts = new DenseMatrix64F(rows, cols);
        // Is a hashmap the best solution, what about collisions etc?
        this.queues = new HashMap<>();
        this.sessionFinished = true;
        this.noOfReplicas = noOfReplicas;
        this.session = new Object();
        this.rows = rows;
        this.cols = cols;
    }

    @Override
    protected boolean update(int srcNodeId, Operation op) {
        MatrixAvgOperation<Double> mop = (MatrixAvgOperation<Double>) op;

        switch (mop.getOpType()) {
            case AVERAGE: return remoteIncludeInAvg(mop);
            default:      return false;
        }
    }

    private boolean remoteIncludeInAvg(MatrixAvgOperation<Double> mop) {
        if(sessionIDs.get(mop.getRow(), mop.getCol()) > mop.getSessionID()) return false;

        if(isCausallyReadyFor(mop)) {
            return makeAverage(mop.getRow(), mop.getCol(), mop.getValue());
        }
        else {
            Tuple3<Long, Long, Double> key = new Tuple3<>(mop.getRow(), mop.getCol(), mop.getSessionID());

            queues.putIfAbsent(key, new ArrayList<>());
            queues.get(key).add(mop);


            return false;
        }
    }

    private boolean makeAverage(long row, long col, Double value) {
        Double count = counts.get(row, col);
        Double avg = matrix.get(row, col);
        /*System.out.println("Average: " + avg);
        System.out.println("New value: " + value);*/

        avg = (avg * count + value) / ++count;

        //System.out.println("New average: " + avg);
        //System.out.println();

        counts.set(row, col, count);
        matrix.set(row, col, avg);

        // If all updates are received a new session starts
        if(counts.get(row, col) == noOfReplicas) {
            sessionFinished = true;
            //sessionIDs.set(row, col, sessionIDs.get(row, col) + 1);
        }

        return true;
    }

    public synchronized void includeInAvg(long row, long col, Double value) {
        // Wait for previous session to finish
        while(!sessionFinished) {
            try {
                session.wait(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // New session
        sessionIDs.set(row, col, sessionIDs.get(row, col) + 1);
        matrix.set(row, col, 0d);
        counts.set(row, col, 0d);

        // New average
        makeAverage(row, col, value);
        applyWaitingOperations(row, col);

        broadcast(new MatrixAvgOperation<>(value, row, col, MatrixAvgOperation.MatrixOpType.AVERAGE, sessionIDs.get(row, col)));
    }

    private boolean applyWaitingOperations(long row, long col) {
        // Apply any waiting operations
        Tuple3<Long, Long, Double> key = new Tuple3<>(row, col, sessionIDs.get(row, col));
        if(queues.containsKey(key)) {
            for (MatrixAvgOperation<Double> op : queues.get(key)) {
                makeAverage(op.getRow(), op.getCol(), op.getValue());
            }
            queues.remove(key);
            return true;
        }

        return false;
    }

    private boolean isCausallyReadyFor(MatrixAvgOperation op) {
        return op.getSessionID() == sessionIDs.get(op.getRow(), op.getCol());
    }

    protected DenseMatrix64F newInstance(long rows, long cols) {
        return new DenseMatrix64F(rows, cols);
    }

    @Override
    public Matrix64F copy() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F copy(long rows, long cols) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void set(long row, long col, Double value) {
        // TODO: this should not be a supported method in production, only for tests
        matrix.set(row, col, value);
        //throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F setDiagonalsToZero() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F setDiagonalsToZero(Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void setArray(Object data) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Double get(long index) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Double get(long row, long col) {
        return matrix.get(row, col);
    }

    @Override
    public Matrix64F getRow(long row) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F getRow(long row, long from, long to) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F getCol(long col) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F getCol(long col, long from, long to) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Object toArray() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnElements(UnaryOperator<Double> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnElements(UnaryOperator<Double> f, Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> B, BinaryOperator<Double> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnElements(Matrix<Double> B, BinaryOperator<Double> f, Matrix<Double> C) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F applyOnNonZeroElements(MatrixElementUnaryOperator<Double> f, Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F assign(Matrix<Double> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F assign(Double aDouble) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F assignRow(long row, Matrix<Double> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F assignColumn(long col, Matrix<Double> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F assign(long rowOffset, long colOffset, Matrix<Double> m) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Double aggregate(BinaryOperator<Double> combiner, UnaryOperator<Double> mapper, Matrix<Double> result) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> f) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F aggregateRows(MatrixAggregation<Double> f, Matrix<Double> result) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Double sum() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F add(Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F add(Matrix<Double> B, Matrix<Double> C) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F addVectorToRows(Matrix<Double> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F addVectorToRows(Matrix<Double> v, Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F addVectorToCols(Matrix<Double> v) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F addVectorToCols(Matrix<Double> v, Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public synchronized Matrix64F sub(Matrix<Double> B) {
        matrix.sub(B, matrix);
        return matrix;
    }

    @Override
    public Matrix64F sub(Matrix<Double> B, Matrix<Double> C) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F mul(Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F mul(Matrix<Double> B, Matrix<Double> C) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F scale(Double a) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F scale(Double a, Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F transpose() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F transpose(Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F invert() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F invert(Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Double norm(int p) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Double dot(Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F concat(Matrix<Double> B) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public Matrix64F concat(Matrix<Double> B, Matrix<Double> C) {
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
