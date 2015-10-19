package de.tuberlin.pserver.types;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Layout;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBase;
import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.math.operations.BinaryOperator;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.operations.MatrixElementUnaryOperator;
import de.tuberlin.pserver.math.operations.UnaryOperator;
import de.tuberlin.pserver.utils.MatrixBuilder;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.RemotePartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DistributedMatrix<V extends Number> implements Matrix<V> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    private final int nodeDOP, nodeID;

    private final IMatrixPartitioner partitioner;

    private final PartitionShape shape;

    private final Format format;

    private final Matrix matrixDelegate;
    
    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedMatrix(final DistributedMatrix m) {
        this(m.programContext, m.matrixDelegate.rows(), m.matrixDelegate.cols(), m.partitioner, m.matrixDelegate.layout(), m.format);
    }

    public DistributedMatrix(final ProgramContext programContext,
                             final long rows,final long cols,
                             final IMatrixPartitioner partitioner,
                             final Layout layout,
                             final Format format) {

        this.programContext = Preconditions.checkNotNull(programContext);
        this.nodeDOP        = programContext.nodeDOP;
        this.nodeID         = programContext.runtimeContext.nodeID;
        this.partitioner    = Preconditions.checkNotNull(partitioner);
        this.shape          = partitioner.getPartitionShape();
        this.format         = format;

        final long _rows = shape.rows;
        final long _cols = shape.cols;

        this.matrixDelegate  = new MatrixBuilder()
                .dimension(_rows, _cols)
                .layout(layout)
                .format(format)
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
    // Private Methods.
    // ---------------------------------------------------

    public long translateRow(final long row) {
        return partitioner.translateGlobalToLocalRow(row);
    }

    public long translateCol(final long col) {
        return partitioner.translateGlobalToLocalCol(col);
    }

        /*public Matrix.RowIterator createLocalRowIterator(final Matrix.RowIterator iter) {
        return new Matrix.RowIterator() {
            @Override public boolean hasNext() { return iter.hasNext(); }
            @Override public void next() { iter.next(); }
            @Override public void nextRandom() { iter.nextRandom(); }
            @Override public double value(long col) { return iter.value(col); }
            @Override public Matrix get() { return iter.get(); }
            @Override public Matrix get(long from, long size) { return iter.get(from, size); }
            @Override public void reset() { iter.reset(); }
            @Override public long size() { return iter.size(); }
            @Override public long rowNum() { return (int) partitioner.translateLocalToGlobalRow(iter.rowNum()); }
        };
    }*/


    private DistributedMatrix constructIntersectingMatrix(DistributedMatrix sourceMatrix, DistributedMatrix targetMatrix, Map<RemotePartition,PartitionShape> partitionMapping) {

        for(Map.Entry<RemotePartition,PartitionShape> entry : partitionMapping.entrySet()) {
            if(entry.getKey().nodeId != nodeID) {
                Matrix subMatrix = null; // TODO: = fetch remote
                targetMatrix.matrixDelegate.assign(entry.getValue().rowOffset, entry.getValue().colOffset, subMatrix);
            }
        }
        /*
        // offer own partition, if any
        RemotePartition ownPartition = remotePartitions.get(nodeID);
        if(ownPartition != null) {
            System.out.println(nodeID + ": " + ownPartition.shape);
            long innerRowOffset = ownPartition.shape.rowOffset - sourceMatrix.shape.rowOffset;
            long innerColOffset = ownPartition.shape.colOffset - sourceMatrix.shape.colOffset;
            Matrix subMatrix = matrix.subMatrix(innerRowOffset, innerColOffset, ownPartition.shape.rows, ownPartition.shape.cols);
            result.matrix.assign(ownPartition.shape.rowOffset, ownPartition.shape.colOffset, subMatrix);
            programContext.runtimeContext.dataManager.pushTo("partialMatrix", subMatrix);
            remotePartitions.remove(ownPartition);
        }
        int n = remotePartitions.size() - (ownPartition != null ? 1 : 0);
        programContext.runtimeContext.dataManager.awaitEvent(ExecutionManager.CallType.SYNC, n, "partialMatrix",
                new DataManager.DataEventHandler() {
                    @Override
                    public void handleDataEvent(int srcNodeID, Object value) {
                        final Matrix remotePartialMatrix = (Matrix) value;
                        RemotePartition remotePartition = remotePartitions.get(srcNodeID);
                        Preconditions.checkNotNull(remotePartition, "Received remote partition from node {} but couldn't find a shape for it.", srcNodeID);
                        result.matrix.assign(remotePartition.shape.rowOffset, remotePartition.shape.colOffset, remotePartialMatrix);
                    }
                }
        );
        */
        return targetMatrix;
    }

    /**
     * Calculates the minimal set of RemotePartitions that need to be fetched in order to construct a Matrix of a given
     * PartitionShape from a given DistributedMatrix. <br>
     * In other words it calculates all PartitionShapes of sourceMatrix that intersect targetShape annotated with the
     * nodeId the shape resides on.
     * SourceMatrix x TargetShape -> {RemotePartitions}
     * @param sourceMatrix The Matrix from which another Matrix shall be constructed
     * @param targetShape The shape of the Matrix that is to be constructed
     * @return The minimal set of RemotePartitions that need to be fetched in order to construct a Matrix of shape targetShape from sourceMatrix
     */
    private static Map<Integer,RemotePartition> getIntersectingRemotePartitions(DistributedMatrix sourceMatrix, PartitionShape targetShape) {
        Map<Integer,RemotePartition> remotePartitions = new HashMap<>(sourceMatrix.nodeDOP);
        // iterate over all nodes, the source matrix is partitioned across
        for(int i : sourceMatrix.partitioner.getNodes()) {
            // get partition shape of (possibly) remote node
            PartitionShape remoteShape = sourceMatrix.partitioner.ofNode(i).getPartitionShape();
            // calculate intersection
            PartitionShape intersection = targetShape.intersect(remoteShape);
            if(intersection != null) { // null if no intersection
                remotePartitions.put(i, new RemotePartition(intersection, i));
            }
            System.out.println(i + ": " + remoteShape + " intersect " + targetShape + " = " + intersection);
        }
        return remotePartitions;
    }

    @Override
    public Matrix<V> copy() {
        return null;
    }

    @Override
    public Matrix<V> copy(long rows, long cols) {
        return null;
    }

    @Override
    public void set(long row, long col, V value) {

    }

    @Override
    public Matrix<V> setDiagonalsToZero() {
        return null;
    }

    @Override
    public Matrix<V> setDiagonalsToZero(Matrix<V> B) {
        return null;
    }

    @Override
    public void setArray(Object data) {

    }

    @Override
    public V get(long index) {
        return null;
    }

    @Override
    public V get(long row, long col) {
        return null;
    }

    @Override
    public Matrix<V> getRow(long row) {
        return null;
    }

    @Override
    public Matrix<V> getRow(long row, long from, long to) {
        return null;
    }

    @Override
    public Matrix<V> getCol(long col) {
        return null;
    }

    @Override
    public Matrix<V> getCol(long col, long from, long to) {
        return null;
    }

    @Override
    public Object toArray() {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(UnaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(UnaryOperator<V> f, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(Matrix<V> B, BinaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(Matrix<V> B, BinaryOperator<V> f, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(MatrixElementUnaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnElements(MatrixElementUnaryOperator<V> f, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> applyOnNonZeroElements(MatrixElementUnaryOperator<V> f) {
        return null;
    }

    @Override
    public Matrix<V> applyOnNonZeroElements(MatrixElementUnaryOperator<V> f, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> assign(Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> assign(V v) {
        return null;
    }

    @Override
    public Matrix<V> assignRow(long row, Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> assignColumn(long col, Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> assign(long rowOffset, long colOffset, Matrix<V> m) {
        return null;
    }

    @Override
    public V aggregate(BinaryOperator<V> combiner, UnaryOperator<V> mapper, Matrix<V> result) {
        return null;
    }

    @Override
    public Matrix<V> aggregateRows(MatrixAggregation<V> f) {
        return null;
    }

    @Override
    public Matrix<V> aggregateRows(MatrixAggregation<V> f, Matrix<V> result) {
        return null;
    }

    @Override
    public V sum() {
        return null;
    }

    @Override
    public Matrix<V> add(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> add(Matrix<V> B, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToRows(Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToRows(Matrix<V> v, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToCols(Matrix<V> v) {
        return null;
    }

    @Override
    public Matrix<V> addVectorToCols(Matrix<V> v, Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> sub(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> sub(Matrix<V> B, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> mul(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> mul(Matrix<V> B, Matrix<V> C) {
        return null;
    }

    @Override
    public Matrix<V> scale(V a) {
        return null;
    }

    @Override
    public Matrix<V> scale(V a, Matrix<V> B) {
        return null;
    }

    public DistributedMatrix transpose() {
        // create transposed local shape
        PartitionShape transposedShape = new PartitionShape(shape.cols, shape.rows, shape.colOffset, shape.rowOffset);
        // get all remote partitions that intersect transposed local shape
        Collection<RemotePartition> remotePartitions = getIntersectingRemotePartitions(this, transposedShape).values();
        // calculate for each remote partition the position where it is to be stored in the resulting matrix
        Map<RemotePartition,PartitionShape> remoteToLocalPartitionMapping = new HashMap<>();
        for(RemotePartition remotePartition : remotePartitions) {
            PartitionShape transposedOffsets = new PartitionShape(remotePartition.shape.rows, remotePartition.shape.cols, remotePartition.shape.colOffset, remotePartition.shape.rowOffset);
            remoteToLocalPartitionMapping.put(remotePartition, transposedOffsets);
        }
        // fetch remote partitions and construct resulting matrix according to position-mapping
        DistributedMatrix result = new DistributedMatrix(programContext, matrixDelegate.cols(), matrixDelegate.cols(), partitioner, matrixDelegate.layout(), format);
        DistributedMatrix remoteView = constructIntersectingMatrix(this, result, remoteToLocalPartitionMapping);
        return remoteView;
    }

    @Override
    public Matrix<V> transpose(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> invert() {
        return null;
    }

    @Override
    public Matrix<V> invert(Matrix<V> B) {
        return null;
    }

    @Override
    public V norm(int p) {
        return null;
    }

    @Override
    public V dot(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> subMatrix(long rowOffset, long colOffset, long rows, long cols) {
        return null;
    }

    @Override
    public Matrix<V> concat(Matrix<V> B) {
        return null;
    }

    @Override
    public Matrix<V> concat(Matrix<V> B, Matrix<V> C) {
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
    public long rows() {
        return 0;
    }

    @Override
    public long cols() {
        return 0;
    }

    @Override
    public long sizeOf() {
        return 0;
    }

    @Override
    public Layout layout() {
        return null;
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

    // ------------------------------------------

}
