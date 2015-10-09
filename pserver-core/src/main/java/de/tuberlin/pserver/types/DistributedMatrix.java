package de.tuberlin.pserver.types;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.matrix.AbstractMatrix;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.utils.MatrixBuilder;
import de.tuberlin.pserver.math.utils.MatrixAggregation;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.runtime.partitioning.IMatrixPartitioner;
import de.tuberlin.pserver.runtime.partitioning.RemotePartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DistributedMatrix extends AbstractMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    private final int nodeDOP, nodeID;

    private final IMatrixPartitioner partitioner;

    private final PartitionShape shape;

    private final Format format;

    private final Matrix matrix;
    
    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedMatrix(final DistributedMatrix m) {
        this(m.programContext, m.rows, m.cols, m.partitioner, m.layout, m.format);
    }

    public DistributedMatrix(final ProgramContext programContext,
                             final long rows,final long cols,
                             final IMatrixPartitioner partitioner,
                             final Layout layout,
                             final Format format) {

        super(rows, cols, layout);

        this.programContext    = Preconditions.checkNotNull(programContext);
        this.nodeDOP        = programContext.nodeDOP;
        this.nodeID         = programContext.runtimeContext.nodeID;
        Preconditions.checkNotNull(partitioner);
        this.partitioner         = partitioner;
        this.shape          = partitioner.getPartitionShape();
        this.format         = format;

        final long _rows = shape.rows;
        final long _cols = shape.cols;
        this.matrix  = new MatrixBuilder()
                .dimension(_rows, _cols)
                .layout(layout)
                .format(format)
                .build();
    }

    // ---------------------------------------------------
    // Public Methods..
    // ---------------------------------------------------

    public long partitionBaseRowOffset() { return shape.rowOffset; }

    public long partitionBaseColOffset() { return shape.colOffset; }

    public long partitionNumRows() { return shape.rows; }

    public long partitionNumCols() { return shape.cols; }

    public Matrix.PartitionShape getPartitionShape() { return shape; }

    @Override public double get(long index) { return matrix.get(index); }

    @Override public double get(long row, long col) { return matrix.get(translateRow(row), translateCol(col)); }

    @Override public void set(long row, long col, double value) { matrix.set(translateRow(row), translateCol(col), value); }

    @Override public Matrix getRow(long row) { return matrix.getRow(translateRow(row)); }

    @Override public Matrix getRow(long row, long from, long to) { return matrix.getRow(translateRow(row), from, to); }

    @Override public Matrix getCol(long col) { return matrix.getCol(translateCol(col)); }

    @Override public Matrix getCol(long col, long from, long to) { return matrix.getCol(translateCol(col), from, to); }

    @Override public Matrix assign(Matrix m) { return matrix.assign(m); }

    @Override public Matrix assign(double v) { return matrix.assign(v); }

    @Override public Matrix assignRow(long row, Matrix v) { return matrix.assignRow(translateRow(row), v); }

    @Override public Matrix assignColumn(long col, Matrix v) { return matrix.assignColumn(translateCol(col), v); }

    @Override public Matrix copy() { return new DistributedMatrix(this); }

    @Override public Matrix subMatrix(long row, long col, long rowSize, long colSize) { return null; }

    @Override public Matrix assign(long rowOffset, long colOffset, Matrix m) { return null; }

    @Override protected Matrix newInstance(long rows, long cols) { throw new UnsupportedOperationException(); }

    @Override public double[] toArray() { return matrix.toArray(); }

    @Override public void setArray(double[] data) { matrix.setArray(data); }

    @Override
    public RowIterator rowIterator() {
        final int start  = (int)translateRow(shape.rowOffset);
        final int end    = (int)(start + shape.rows);
        return createLocalRowIterator(matrix.rowIterator(start, end));
    }

    @Override
    public RowIterator rowIterator(int startRow, int endRow) {
        final int start  = (int)translateRow(startRow);
        final int end    = (int)translateRow(endRow);
        return createLocalRowIterator(matrix.rowIterator(start, end));
    }

    // ---------------------------------------------------
    // Distributed Operations - Synchronous!
    // ---------------------------------------------------

    public Matrix aggregateRows(final MatrixAggregation f) {
//        switch (partitionType) {
//            case ROW_PARTITIONED: {
//
//                final Matrix partialAgg = new MatrixBuilder().dimension(1, shape.rows).layout(layout).format(format).build();
//                final Matrix totalAgg = new MatrixBuilder().dimension(1, rows).layout(layout).format(format).build();
//                final RowIterator iter = rowIterator();
//                while(iter.hasNext()) {
//                    iter.next();
//                    final int row = iter.rowNum();
//                    final double value = f.apply(getRow(row));
//                    partialAgg.set(0, row - shape.rowOffset, value);
//                    totalAgg.set(0, row, value);
//                }
//
//                programContext.runtimeContext.dataManager.pushTo("rowAgg", partialAgg);
//                final int n = programContext.programContext.nodeDOP - 1;
//                programContext.runtimeContext.dataManager.awaitEvent(ExecutionManager.CallType.SYNC, n, "rowAgg",
//                        new DataManager.DataEventHandler() {
//                            @Override
//                            public void handleDataEvent(int srcNodeID, Object value) {
//                                final Matrix remotePartialAgg = (Matrix)value;
//                                final long offset = rows / programContext.programContext.nodeDOP * srcNodeID;
//                                for (int i = 0; i < remotePartialAgg.rows(); ++i)
//                                   totalAgg.set(0, offset + i, remotePartialAgg.get(0, i));
//                            }
//                        }
//                );
//                return totalAgg;
//            }
//            case COLUMN_PARTITIONED: throw new IllegalStateException();
//            case BLOCK_PARTITIONED: throw new IllegalStateException();
//            default: throw new IllegalStateException();
//        }
        return null;
    }

    public DistributedMatrix collectRemotePartitions() {
//        if (!completeMatrix)
//            throw new IllegalStateException();
//        switch (partitionType) {
//            case ROW_PARTITIONED: {
//                Matrix partialMatrix = matrix.subMatrix(shape.rowOffset, shape.colOffset, shape.rows, shape.cols);
//                programContext.runtimeContext.dataManager.pushTo("partialMatrix", partialMatrix);
//                final int n = programContext.programContext.nodeDOP - 1;
//                programContext.runtimeContext.dataManager.awaitEvent(ExecutionManager.CallType.SYNC, n, "partialMatrix",
//                        new DataManager.DataEventHandler() {
//                            @Override
//                            public void handleDataEvent(int srcNodeID, Object value) {
//                                final Matrix remotePartialMatrix = (Matrix)value;
//                                matrix.assign(srcNodeID * shape.rows, 0, remotePartialMatrix);
//                            }
//                        }
//                );
//            } break;
//            case COLUMN_PARTITIONED: throw new IllegalStateException();
//            case BLOCK_PARTITIONED: throw new IllegalStateException();
//            default: throw new IllegalStateException();
//        }
//        return this;
        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    public long translateRow(final long row) {
        return partitioner.translateGlobalToLocalRow(row);
    }

    public long translateCol(final long col) {
        return partitioner.translateGlobalToLocalCol(col);
    }

    public Matrix.RowIterator createLocalRowIterator(final Matrix.RowIterator iter) {
        return new Matrix.RowIterator() {
            @Override public boolean hasNext() { return iter.hasNext(); }
            @Override public void next() { iter.next(); }
            @Override public void nextRandom() { iter.nextRandom(); }
            @Override public double value(long col) { return iter.value(col); }
            @Override public Matrix get() { return iter.get(); }
            @Override public Matrix get(int from, int size) { return iter.get(from, size); }
            @Override public void reset() { iter.reset(); }
            @Override public long rows() { return iter.rows(); }
            @Override public long cols() { return iter.cols(); }
            @Override public int rowNum() { return (int) partitioner.translateLocalToGlobalRow(iter.rowNum()); }
        };
    }

    private DistributedMatrix constructIntersectingMatrix(DistributedMatrix sourceMatrix, DistributedMatrix targetMatrix, Map<RemotePartition,PartitionShape> partitionMapping) {

        for(Map.Entry<RemotePartition,PartitionShape> entry : partitionMapping.entrySet()) {
            if(entry.getKey().nodeId != nodeID) {
                Matrix subMatrix = null; // TODO: = fetch remote
                targetMatrix.matrix.assign(entry.getValue().rowOffset, entry.getValue().colOffset, subMatrix);
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
    public Matrix transpose() {
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
        DistributedMatrix result = new DistributedMatrix(programContext, cols, rows, partitioner, layout, format);
        DistributedMatrix remoteView = constructIntersectingMatrix(this, result, remoteToLocalPartitionMapping);
        return remoteView;
    }
}
