package de.tuberlin.pserver.matrix;

import de.tuberlin.pserver.crdt.operations.Operation;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

public class CRDTDenseMatrix32F<T> extends AbstractCRDTMatrix<T> implements Matrix32F {
    private DenseMatrix32F matrix;

    // S3 Vector divided into seperate matrices
    private DenseMatrix32F vectorClockSums;
    private DenseMatrix32F sessionIDs;
    private DenseMatrix32F siteIDs;
    //private final S3Vector[] matrixInfo;

    // Copy constructor
    protected CRDTDenseMatrix32F(DenseMatrix32F m, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.matrix = new DenseMatrix32F(m);
        this.vectorClockSums = new DenseMatrix32F(m.rows(), m.cols());
        this.sessionIDs = new DenseMatrix32F(m.rows(), m.cols());
        this.siteIDs = new DenseMatrix32F(m.rows(), m.cols());
        //this.matrixInfo = new S3Vector[Math.toIntExact(matrix.rows() * matrix.cols())];
    }

    protected CRDTDenseMatrix32F(final long rows, final long cols, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.matrix = new DenseMatrix32F(rows, cols);
        this.vectorClockSums = new DenseMatrix32F(rows, cols);
        this.sessionIDs = new DenseMatrix32F(rows, cols);
        this.siteIDs = new DenseMatrix32F(rows, cols);
        //this.matrixInfo = new S3Vector[Math.toIntExact(matrix.rows() * matrix.cols())];
    }

    protected CRDTDenseMatrix32F(final long rows, final long cols, float[] data, String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);
        this.matrix = new DenseMatrix32F(rows, cols, data);
        this.vectorClockSums = new DenseMatrix32F(rows, cols);
        this.sessionIDs = new DenseMatrix32F(rows, cols);
        this.siteIDs = new DenseMatrix32F(rows, cols);
        //this.matrixInfo = new S3Vector[Math.toIntExact(matrix.rows() * matrix.cols())];
    }

    private long getArrayIndex(long row, long col){
        return row * matrix.cols() + col;
    }

    protected boolean isCausallyReadyFor(MatrixOperation op) {
        // TODO: what about if sum > vectorClockSum => must the operation be purged from queue?
        // TODO: this needs verification
        //System.out.println();
        //System.out.println("Local vector clock: " + vectorClock[Math.toIntExact(op.getS3Vector().getSiteID())]);
        //System.out.println("Remote vector clock: " + op.getVectorClock()[Math.toIntExact(op.getS3Vector().getSiteID())]);
        return vectorClock[Math.toIntExact(op.getS3Vector().getSiteID())] == (op.getVectorClock()[Math.toIntExact(op.getS3Vector().getSiteID())]-1);
    }

    //TODO: synchronized?
    @Override
    protected boolean update(int srcNodeId, Operation<?> op) {
        MatrixOperation<Float> mop = (MatrixOperation<Float>) op;

        switch(mop.getOpType()) {
            case SET:
                return remoteSet(mop.getRow(), mop.getCol(), mop.getValue(), mop.getS3Vector());
            case SET_DIAGONALS_ZERO:
                return remoteSetDiagonalToZero(mop);
            case TRANSPOSE:
                return remoteTranspose();
            default:
                throw new IllegalArgumentException("GCounter CRDTs do not allow the " + op.getOperationType() + " operation.");
        }

    }


    private synchronized boolean remoteSet(long row, long col, float value, S3Vector s3) {
        if(takesPrecedence(row, col, s3)) {
            matrix.set(row, col, value);
            vectorClockSums.set(row, col, (float) s3.getVectorClockSum());
            sessionIDs.set(row, col, (float) s3.getSessionID());
            siteIDs.set(row, col, (float) s3.getSiteID());

            return true;
        }
        return false;
    }

    private synchronized boolean remoteSetDiagonalToZero(MatrixOperation<Float> op) {
        for(int diag = 0; diag < matrix.rows() && diag < matrix.cols(); diag++) {
            remoteSet(diag, diag, 0, op.getS3Vector());
        }
        return true;
    }

    private synchronized boolean remoteTranspose() {
        vectorClockSums = (DenseMatrix32F) vectorClockSums.transpose();
        sessionIDs = (DenseMatrix32F) sessionIDs.transpose();
        siteIDs = (DenseMatrix32F) siteIDs.transpose();
        matrix = (DenseMatrix32F) matrix.transpose();

        return true;
    }

    private boolean takesPrecedence(long row, long col, S3Vector s3) {
        System.out.println("Comparing " + vectorClockSums.get(row, col) + "<" + s3.getVectorClockSum());
        boolean blub = vectorClockSums.get(row, col) < s3.getVectorClockSum();
        System.out.println("Result: " + blub);

        if(sessionIDs.get(row, col) < s3.getSessionID()) return true;
        else if(sessionIDs.get(row, col) > s3.getSessionID()) return false;

        if(vectorClockSums.get(row, col) < s3.getVectorClockSum()) return true;
        else if(vectorClockSums.get(row, col) > s3.getVectorClockSum()) return false;

        // In this case the operations are concurrent
        // TODO: allow user defined resolution of concurrent updates
        if(siteIDs.get(row, col) < s3.getSiteID()) return true;

        return false;
    }

    @Override
    public Matrix32F copy() {
        return matrix.copy();
    }

    @Override
    public Matrix32F copy(long rows, long cols) {
        return matrix.copy(rows, cols);
    }

    /*public void newRunningAverage(long row, long col, Float value) {
        matrix.set(row, col, value);
        avgCount = 1;
        long[] clock = increaseVectorClock();
        S3Vector s3 = new S3Vector(clock, ++sessionID, nodeId);
        broadcast(new MatrixOperation<>(value, row, col, MatrixOperation.MatrixOpType.AVG, clock, s3));
    }

    public void addToRunningAverage(long row, long col, Float value) {
        System.out.println("[DEBUG:" + nodeId + "] Calculating avg: " + matrix.get(row, col)+ "*" +avgCount + "+" + value
        + "/" + avgCount +1);
        matrix.set(row, col, (matrix.get(row, col) * avgCount + value) / ++avgCount);
    }*/

    @Override
    public synchronized void set(long row, long col, Float value) {
        long[] clock = increaseVectorClock();
        S3Vector s3 = new S3Vector(clock, sessionID, nodeId );

        matrix.set(row, col, value);
        //TODO: get rid of these casts!! (new matrix types or better generics of matrices)
        vectorClockSums.set(row, col, (float)s3.getVectorClockSum());
        sessionIDs.set(row, col, (float)s3.getSessionID());
        siteIDs.set(row, col, (float)s3.getSiteID());
        broadcast(new MatrixOperation<>(value, row, col, MatrixOperation.MatrixOpType.SET, clock, s3));

    }

    @Override
    public synchronized Matrix32F setDiagonalsToZero() {
        long[] clock = increaseVectorClock();
        S3Vector s3 = new S3Vector(clock, sessionID, nodeId );

        for(int diag = 0; diag < matrix.rows() && diag < matrix.cols(); diag++) {
            vectorClockSums.set(diag, diag, (float) s3.getVectorClockSum());
            sessionIDs.set(diag, diag, (float) s3.getSessionID());
            siteIDs.set(diag, diag, (float) s3.getSiteID());
        }

        matrix = (DenseMatrix32F) matrix.setDiagonalsToZero();
        broadcast(new MatrixOperation<>(null, 0, 0, MatrixOperation.MatrixOpType.SET_DIAGONALS_ZERO, clock, s3));

        return matrix;
    }

    @Override
    public Matrix32F setDiagonalsToZero(Matrix<Float> B) {
        // Not supported because it doesn't make sense to send a whole matrix over the network...
        throw new UnsupportedOperationException("This operation is not supported for CRDTs");
    }

    @Override
    public void setArray(Object data) {
        // Not supported because it doesn't make sense to send a whole matrix over the network...
        throw new UnsupportedOperationException("This operation is not supported for CRDTs");
    }

    @Override
    public Float get(long index) {
        return matrix.get(index);
    }

    @Override
    public Float get(long row, long col) {
        return matrix.get(row, col);
    }

    @Override
    public Matrix32F getRow(long row) {
        return matrix.getRow(row);
    }

    @Override
    public Matrix32F getRow(long row, long from, long to) {
        return matrix.getRow(row, from, to);
    }

    @Override
    public Matrix32F getCol(long col) {
        return matrix.getCol(col);
    }

    @Override
    public Matrix32F getCol(long col, long from, long to) {
        return matrix.getCol(col, from, to);
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(UnaryOperator<Float> f, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(Matrix<Float> B, BinaryOperator<Float> f, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f) {
        return null;
    }

    @Override
    public Matrix32F applyOnNonZeroElements(MatrixElementUnaryOperator<Float> f, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F assign(Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F assign(Float aFloat) {
        return null;
    }

    @Override
    public Matrix32F assignRow(long row, Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F assignColumn(long col, Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F assign(long rowOffset, long colOffset, Matrix<Float> m) {
        return null;
    }

    @Override
    public Float aggregate(BinaryOperator<Float> combiner, UnaryOperator<Float> mapper, Matrix<Float> result) {
        return null;
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f) {
        return null;
    }

    @Override
    public Matrix32F aggregateRows(MatrixAggregation<Float> f, Matrix<Float> result) {
        return null;
    }

    @Override
    public Float sum() {
        return null;
    }

    @Override
    public Matrix32F add(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F add(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F addVectorToRows(Matrix<Float> v, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v) {
        return null;
    }

    @Override
    public Matrix32F addVectorToCols(Matrix<Float> v, Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F sub(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F sub(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F mul(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F mul(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    @Override
    public Matrix32F scale(Float a) {
        return null;
    }

    @Override
    public Matrix32F scale(Float a, Matrix<Float> B) {
        return null;
    }

    // TODO: This is still eperimental
    @Override
    public synchronized Matrix32F transpose() {
        long[] clock = increaseVectorClock();
        S3Vector s3 = new S3Vector(clock, increaseSessionID(), nodeId );

        vectorClockSums = (DenseMatrix32F) vectorClockSums.transpose();
        sessionIDs = (DenseMatrix32F) sessionIDs.transpose();
        siteIDs = (DenseMatrix32F) siteIDs.transpose();
        matrix = (DenseMatrix32F) matrix.transpose();

        broadcast(new MatrixOperation<>(null, 0, 0, MatrixOperation.MatrixOpType.TRANSPOSE, clock, s3));

        return matrix;
    }

    @Override
    public Matrix32F transpose(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F invert() {
        return null;
    }

    @Override
    public Matrix32F invert(Matrix<Float> B) {
        return null;
    }

    @Override
    public Float norm(int p) {
        return null;
    }

    @Override
    public Float dot(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        return null;
    }

    @Override
    public Matrix32F concat(Matrix<Float> B) {
        return null;
    }

    @Override
    public Matrix32F concat(Matrix<Float> B, Matrix<Float> C) {
        return null;
    }

    @Override
    public RowIterator rowIterator() {
        return null;
    }

    @Override
    public RowIterator rowIterator(long startRow, long endRow) {
        return null;
    }

    @Override
    public Object toArray() {
        return matrix.toArray();
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
        return 0;
    }

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public void setOwner(Object owner) {

    }

    @Override
    public Object getOwner() {
        return null;
    }
}