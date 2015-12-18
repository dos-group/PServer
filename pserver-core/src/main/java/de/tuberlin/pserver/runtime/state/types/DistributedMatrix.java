package de.tuberlin.pserver.runtime.state.types;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.partitioning.PartitionShape;
import de.tuberlin.pserver.math.operations.MatrixAggregation;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.Format;
import de.tuberlin.pserver.runtime.state.MatrixBuilder;
import de.tuberlin.pserver.runtime.state.RemotePartition;
import de.tuberlin.pserver.runtime.state.partitioner.IMatrixPartitioner;

import java.util.*;

public class DistributedMatrix {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    private final int nodeDOP, nodeID;

    private final IMatrixPartitioner partitioner;

    private final PartitionShape shape;

    private final Format format;

    private final Matrix matrix;

    private static final String GET_BLOCK = "get_block";

    private long rows, cols;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedMatrix(final ProgramContext programContext,
                             final long rows,final long cols,
                             final IMatrixPartitioner partitioner,
                             final Format format) {

        this.programContext    = Preconditions.checkNotNull(programContext);
        this.nodeDOP        = programContext.nodeDOP;
        this.nodeID         = programContext.runtimeContext.nodeID;
        this.partitioner    = Preconditions.checkNotNull(partitioner);
        this.shape          = partitioner.getPartitionShape();
        this.format         = format;

        final long _rows = shape.rows;
        final long _cols = shape.cols;
        this.matrix  = new MatrixBuilder()
                .dimension(_rows, _cols)
                .format(format)
                .build();

        programContext.runtimeContext.runtimeManager.registerPullHandler(GET_BLOCK, new RuntimeManager.PullHandler() {
            @Override
            public Object handlePull(String name, Object requestParam) {
                Preconditions.checkArgument(name.equals(GET_BLOCK));
                Preconditions.checkArgument(requestParam instanceof Set, requestParam.getClass());
                Set<Tuple2<RemotePartition,PartitionShape>> requests = (Set) requestParam;
                Set<Tuple2<PartitionShape,Matrix>> result = new HashSet<>();
                for(Object requestObj : requests) {
                    Preconditions.checkState(requestObj instanceof Tuple2, requestObj.getClass());
                    Tuple2 requestTuple = (Tuple2) requestObj;
                    Object key = requestTuple._1;
                    System.out.println("1: " + requestTuple._1.getClass() + "; " + "2: " + requestTuple._2.getClass());
                    Preconditions.checkState(key instanceof RemotePartition, key.getClass());
                    RemotePartition remotePartition = (RemotePartition) key;
                    Object value = requestTuple._2;
                    Preconditions.checkState(value instanceof PartitionShape, value.getClass());
                    PartitionShape partitionShape = (PartitionShape) value;
                    PartitionShape requestPartition = remotePartition.shape;
                    Matrix subMatrix = matrix.subMatrix(requestPartition.rows, requestPartition.cols, requestPartition.rowOffset - shape.rowOffset, requestPartition.colOffset - shape.colOffset);
                    System.out.println("cut shape " + requestPartition + " from " + matrix.toString() + " = " + subMatrix.toString());
                    result.add(new Tuple2<>(partitionShape, subMatrix));
                }
                return result;
            }
        });

    }

    // ---------------------------------------------------
    // Public Methods..
    // ---------------------------------------------------

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

    private Set<Tuple2<PartitionShape,Matrix>> fetchRemotePartitions(Set<Tuple2<RemotePartition,PartitionShape>> remotePartitions) {

        // require that all remotePartitions are to be fetched from the same node
        Preconditions.checkNotNull(remotePartitions);
        Preconditions.checkArgument(remotePartitions.size() > 0);
        int remoteNodeId = -1;
        for(Tuple2<RemotePartition,PartitionShape> entry : remotePartitions) {
            if(remoteNodeId == -1) {
                remoteNodeId = entry._1.nodeId;
            }
            else {
                Preconditions.checkState(remoteNodeId == entry._1.nodeId);
            }
        }

        // result buffer
        Set<Tuple2<PartitionShape,Matrix>> result;
        // if remotePartitions are from own node, just cut them out and return
        if(remoteNodeId == nodeID) {
            result = new HashSet<>();
            for(Tuple2<RemotePartition,PartitionShape> remoteShape : remotePartitions) {
                PartitionShape partitionToCut = remoteShape._1.shape;
                System.out.println("["+nodeID+"] want to cut " + partitionToCut + " from " + shape + " -> ("+matrix.rows()+","+matrix.cols()+")");
                System.out.println(String.format("matrix.subMatrix(%d, %d, %d, %d)", partitionToCut.rows, partitionToCut.cols, partitionToCut.rowOffset - shape.rowOffset, partitionToCut.colOffset - shape.colOffset));
                Matrix subMatrix = matrix.subMatrix(partitionToCut.rowOffset - shape.rowOffset, partitionToCut.colOffset - shape.colOffset, partitionToCut.rows, partitionToCut.cols);
                Preconditions.checkState(subMatrix.rows() == partitionToCut.rows);
                Preconditions.checkState(subMatrix.cols() == partitionToCut.cols);
                result.add(new Tuple2<>(remoteShape._2, subMatrix));
            }
        }
        // if remotePartitions are from remote node, fetch them
        else {
            Object[] response = programContext.runtimeContext.runtimeManager.pull(GET_BLOCK, remotePartitions, new int[] {remoteNodeId});
            Preconditions.checkNotNull(response);
            Preconditions.checkState(response.length == 1);
            result = (Set<Tuple2<PartitionShape,Matrix>>) response[0];
        }
        return result;

    }

    private DistributedMatrix constructMatrixFromRemotePartitions(Map<RemotePartition, PartitionShape> partitionMapping, DistributedMatrix targetMatrix) {
        Map<Integer,Set<Tuple2<RemotePartition,PartitionShape>>> remotePartitionsPerNode = new HashMap<>();
        for(Map.Entry<RemotePartition,PartitionShape> entry : partitionMapping.entrySet()) {
            Set<Tuple2<RemotePartition,PartitionShape>> partitions = remotePartitionsPerNode.get(entry.getKey().nodeId);
            if (partitions == null) {
                partitions = new HashSet<>();
                remotePartitionsPerNode.put(entry.getKey().nodeId, partitions);
            }
            partitions.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        for(Set<Tuple2<RemotePartition,PartitionShape>> entry : remotePartitionsPerNode.values()) {
            Set<Tuple2<PartitionShape,Matrix>> remotePartitions = fetchRemotePartitions(entry);
            for(Tuple2<PartitionShape,Matrix> remotePartition : remotePartitions) {
                PartitionShape targetPartitionShape = remotePartition._1;
                Matrix subMatrix = remotePartition._2;
                System.out.println("put matrix: " + subMatrix.toString() + " to position: " + targetPartitionShape);
                System.out.println(String.format("targetMatrix.matrix.assign(%d, %d, matrix)", targetPartitionShape.rowOffset - targetMatrix.shape.rowOffset, targetPartitionShape.colOffset - targetMatrix.shape.colOffset));
                System.out.println("targetShape: " + targetPartitionShape + "; subMatrix.rows=" + subMatrix.rows() +"; subMatrix.cols=" + subMatrix.cols());
                targetMatrix.matrix.assign(targetPartitionShape.rowOffset - targetMatrix.shape.rowOffset, targetPartitionShape.colOffset - targetMatrix.shape.colOffset, subMatrix);
            }
        }
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
            //System.out.println(i + ": " + remoteShape + " intersect " + targetShape + " = " + intersection);
        }
        return remotePartitions;
    }

    /**
     * This is currently only supported for quadratic matrices!<br>
     * Non-quadratic matrices would require column-based partitioning.
     * @return
     */
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
            //System.out.println(nodeID + " has to fetch " + remotePartition.shape + " from node " + remotePartition.nodeId + " and put it to " + transposedOffsets);
        }

        // fetch remote partitions and construct resulting matrix according to position-mapping
        DistributedMatrix result = new DistributedMatrix(programContext, cols, rows, partitioner, format);
        System.out.println(result.matrix.rows() + " - " + result.matrix.cols());
        DistributedMatrix remoteView = constructMatrixFromRemotePartitions(remoteToLocalPartitionMapping, result);
        return remoteView;
    }
}
