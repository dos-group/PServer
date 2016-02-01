package de.tuberlin.pserver.regression;


import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.math.utils.Utils;
import de.tuberlin.pserver.matrix.crdt.MatrixAvgOperation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.io.Serializable;
import java.util.*;

/**
 * Ideas:
 *
 * 1. Exact solution.
 * 2. Weight every new input by 1/numOfReplicas (and wait until the value is within a certain range)
 * 3. Exact solution but without session (just always include the newest updates)
 * 4.
 */

public class NoSessionAvgRegressionDenseMatrix32F extends AbstractAvgReplicatedMatrix implements Serializable, Matrix32F {//, AvgMatrix32F {
    private final long rows;
    private final long cols;
    private DenseMatrix32F matrix;
    private final DenseMatrix32F counts;
    private final int noOfReplicas;
    private final Queue<MatrixAvgOperation> queue;
    private volatile boolean converged;
    // private volatile boolean sessionFinished;
    //private final Object session;
    //private volatile boolean isLocked;
    private final int nodeID;

    public NoSessionAvgRegressionDenseMatrix32F(long rows, long cols, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.matrix = new DenseMatrix32F(rows, cols);
        this.counts = new DenseMatrix32F(rows, cols);
        this.queue =  new LinkedList<>();//Collections.synchronizedList(new LinkedList(...));
        this.noOfReplicas = noOfReplicas;
        this.rows = rows;
        this.cols = cols;
        this.nodeID = programContext.nodeID;
        this.converged = false;
        //this.isLocked = false;
    }

    @Override
    public String toString() {
        return nodeID + " | " + matrix.toString();
    }

    public Matrix32F subAndBroadcast(Matrix<Float> B, Matrix<Float> C) {
        if(!this.equals(C)) throw new IllegalArgumentException("CRDTs must apply operations on themselves.");
        Utils.checkShapeEqual(this, B);
        return this.applyOnElementsAndBroadcast(B, (x, y) -> x - y, this);
    }

    private void setAndBroadcast(long row, long col, Float value) {
        /*System.out.println("Check A");
        while(!sessionFinished) {
            try {
                synchronized(session) {
                    session.wait(10);
                }
                //Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Check B");*/

        // New session
        //matrix.set(row, col, 0f);
        counts.set(row, col, 1f);

        matrix.set(row, col, value);
        this.applyWaitingOperations();

        broadcast(new MatrixAvgOperation<>(value, row, col, MatrixAvgOperation.MatrixOpType.AVERAGE, null));
    };

    public Matrix32F applyOnElementsAndBroadcast(Matrix<Float> B, BinaryOperator<Float> f, Matrix<Float> C) {
        if(!this.equals(C)) throw new IllegalArgumentException("CRDTs must apply operations on themselves.");
        Utils.checkShapeEqual(this, B);

        for (int i = 0; i < rows(); ++i) {
            for (int j = 0; j < cols(); ++j) {
                this.setAndBroadcast(i, j, f.apply(this.get(i, j), B.get(i, j)));
            }
        }
        return this;
    }

   /* @Override
    protected boolean remoteIncludeInAvg(MatrixAvgOperation<Float> op) {
        //applyWaitingOperations();
        //return makeAverage(mop.getRow(), mop.getCol(), mop.getValue());
        queue.add(op);
        return true;
    }*/

    protected boolean makeAverage(long row, long col, Float value) {
        Float count = counts.get(row, col);
        Float avg = matrix.get(row, col);
        /*System.out.println("Node: " + nodeId);
        System.out.println("Old Average: " + avg);
        System.out.println("New value: " + value);*/

        avg = (avg * count + value) / ++count;

        //System.out.println("New average: " + avg);
        //System.out.println();

        counts.set(row, col, count);
        matrix.set(row, col, avg);

        // If all updates are received a new session starts
        /*System.out.println("Count: "+counts.get(row, col));
        System.out.println("No of replicas: "+noOfReplicas);
        System.out.println(counts.get(row, col) == noOfReplicas);*/

        return true;
    }

    //@Override
   /* public synchronized void includeInAvg(long row, long col, Float value) {
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
        matrix.set(row, col, 0f);
        counts.set(row, col, 0f);

        // New average
        makeAverage(row, col, value);
        applyWaitingOperations(row, col);

        broadcast(new MatrixAvgOperation<>(value, row, col, MatrixAvgOperation.MatrixOpType.AVERAGE, sessionIDs.get(row, col)));
    }*/


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
        // TODO: is this correct?
        //return matrix.get((index / cols), index % cols);

        return matrix.get(index);
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
                matrix.set(i, j, f.apply(this.get(i, j)));
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
                matrix.set(i, j, f.apply(this.get(i, j), B.get(i, j)));
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
        //throw new UnsupportedOperationException("Operation not supported");
        return sub(B, this);
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
    public Matrix32F scale(final Float a) {
        //throw new UnsupportedOperationException("Operation not supported");
        return matrix.scale(a);
    }

    @Override
    public Matrix32F scale(final Float a, final Matrix<Float> B) {
        throw new UnsupportedOperationException("Operation not supported");

        /*if(!this.equals(B)) throw new IllegalArgumentException("CRDTs must apply operations on themselves.");
        Utils.checkShapeEqual(this, B);
        return applyOnElements(x -> a * x, this);*/
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
