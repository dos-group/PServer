package de.tuberlin.pserver.runtime.partitioning;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;

import java.util.Arrays;
import java.util.stream.IntStream;

public class MatrixByRowPartitioner extends IMatrixPartitioner {

    private Matrix.PartitionShape shape;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixByRowPartitioner(long rows, long cols, int nodeId, int[] atNodes) {
        super(rows, cols, nodeId, atNodes);
        shape = getPartitionShape();
    }

    public MatrixByRowPartitioner(long rows, long cols, int nodeId, int numNodes) {
        this(rows, cols, nodeId, IntStream.iterate(0, x -> x + 1).limit(numNodes).toArray());
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int getPartitionOfEntry(final MatrixEntry entry) {
        double numOfRowsPerInstance = (double) rows / atNodes.length;
        int partition = (int) (entry.getRow() / numOfRowsPerInstance);
        if(partition >= atNodes.length) {
            throw new IllegalStateException("The calculated partition id (row = "+entry.getRow()+", rows = "+rows+", numNodes = "+atNodes.length+") -> " + partition + " must not exceed numNodes.");
        }
        return partition;
    }

    @Override
    public Matrix.PartitionShape getPartitionShape() {
        if(shape == null) {
            double rowsPerNode = (double) rows / atNodes.length;
            long rowOffset = (int) Math.ceil(rowsPerNode * nodeId);
            long numRows = (int) (Math.ceil(rowsPerNode * (nodeId + 1)) - rowOffset);
            shape = new Matrix.PartitionShape(numRows, cols, rowOffset, 0);
        }
        return shape;
    }

    @Override
    public long translateGlobalToLocalRow(long row) {
        long firstRow = shape.rowOffset;
        long lastRow  = shape.rowOffset + shape.rows - 1;
        if(row < firstRow || row > lastRow) {
            throw new IllegalArgumentException(String.format("Can not translate row: %d. Not in current partitions row range: [%d, %d]", row, firstRow, lastRow));
        }
        return row - firstRow;
    }

    @Override
    public long translateGlobalToLocalCol(long col) {
        return col;
    }

    @Override
    public long translateLocalToGlobalRow(long row) throws IllegalArgumentException {
        if(row > shape.rows - 1) {
            throw new IllegalArgumentException(String.format("Can not translate row: %d. Not in allowed range: [%d, %d]", row, 0, shape.rows - 1));
        }
        return row + shape.rowOffset;
    }

    @Override
    public long translateLocalToGlobalCol(long col) throws IllegalArgumentException {
        return col;
    }

    @Override
    public int getNumRowPartitions() {
        return atNodes.length;
    }

    @Override
    public int getNumColPartitions() {
        return 1;
    }

    @Override
    public IMatrixPartitioner ofNode(int nodeId) {
        Preconditions.checkArgument(Arrays.asList(atNodes).contains(nodeId), "Can not construct MatrixByRowPartitioner of node '%d' because it is part of this partitioning. Participating nodes are: %s", nodeId, Arrays.toString(atNodes));
        return new MatrixByRowPartitioner(rows, cols, nodeId, atNodes);
    }

    //    // TODO: onvert this into a unit test
//    public static void main(String[] args) {
//
//        try {
//            MatrixByRowPartitioner test = MatrixByRowPartitioner.class.getDeclaredConstructor(PartitioningConfig.class).newInstance(new PartitioningConfig(1,1,1,1));
//            System.out.println(test.getPartitionShape());
//        } catch (InstantiationException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (InvocationTargetException e) {
//            e.printStackTrace();
//        } catch (NoSuchMethodException e) {
//            e.printStackTrace();
//        }
//
//        long rows = 10;
//        int numNodes = 4;
//        MutableMatrixEntry dummyEntry = new MutableMatrixEntry(-1, -1, Double.NaN);
//        MatrixByRowPartitioner[] partitioners = new MatrixByRowPartitioner[numNodes];
//        Matrix.PartitionShape[] partitionShapes = new Matrix.PartitionShape[numNodes];
//        for (int node = 0; node < numNodes; node++) {
//            partitioners[node] = new MatrixByRowPartitioner(node, numNodes, rows, rows);
//            System.out.println(node + " -> " + partitioners[node].getPartitionShape());
//        }
//        for (int row = 0; row < rows; row++) {
//            int[] partitions = new int[numNodes];
//            int firstPartition = -1;
//            for (int node = 0; node < numNodes; node++) {
//                partitions[node] = partitioners[node].getPartitionOfEntry(dummyEntry.set(row, -1, Double.NaN));
//                partitionShapes[node] = partitioners[node].getPartitionShape();
//                if(node == 0) {
//                    firstPartition = partitions[node];
//                }
//                Matrix.PartitionShape shape = partitionShapes[node];
//                if(partitions[node] == node && (row >= shape.rows + shape.rowOffset || row < shape.rowOffset)) {
//                    System.out.println("FUCKED UP! node: " + node + ", row: "+ row);
//                    System.out.println("row does not fit in partition: " + shape);
//                }
//            }
//            System.out.println("row " + row + " -> " + Arrays.toString(partitions));
//            for (int node = 0; node < numNodes; node++) {
//                if(partitions[node] != firstPartition) {
//                    System.out.println("FUCKED UP");
//                    System.out.println(Arrays.toString(partitions));
//                }
//            }
//        }
//    }

}
