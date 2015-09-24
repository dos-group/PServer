package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MutableMatrixEntry;

import java.util.Arrays;
import java.util.stream.IntStream;

public class MatrixByRowPartitioner implements IMatrixPartitioner {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int nodeID;

    //private final int numNodes;

    private final long rows;

    private final long cols;

    private final int[] atNodes;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixByRowPartitioner(int nodeID, int[] atNodes, long rows, long cols) {
        this.nodeID   = nodeID;
        this.atNodes  = atNodes;
        this.rows     = rows;
        this.cols     = cols;
    }

    public MatrixByRowPartitioner(int nodeID, int numNodes, long rows, long cols) {
        this(nodeID, IntStream.iterate(0, x -> x + 1).limit(numNodes).toArray(), rows, cols);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int getPartitionOfEntry(final MatrixEntry entry) {
        double numOfRowsPerInstance = (double) rows / atNodes.length;
        int partition = (int) (entry.getRow() / numOfRowsPerInstance);
        if(partition >= atNodes.length) {
            throw new IllegalStateException("The calculated partition id (row = "+entry.getRow()+", rows = "
                    + rows + ", numNodes = " + atNodes.length +") -> " + partition + " must not exceed numNodes.");
        }
        return atNodes[partition];
    }

    @Override
    public Matrix.PartitionShape getPartitionShape() {
        double rowsPerNode = (double) rows / atNodes.length;
        long rowOffset = (int) Math.ceil(rowsPerNode * nodeID);
        long numRows = (int) (Math.ceil(rowsPerNode * (nodeID + 1)) - rowOffset);
        return new Matrix.PartitionShape(numRows, cols, rowOffset, 0);
    }

    // TODO: onvert this into a unit test
    public static void main(String[] args) {
        long rows = 10;
        int numNodes = 4;
        MutableMatrixEntry dummyEntry = new MutableMatrixEntry(-1, -1, Double.NaN);
        MatrixByRowPartitioner[] partitioners = new MatrixByRowPartitioner[numNodes];
        Matrix.PartitionShape[] partitionShapes = new Matrix.PartitionShape[numNodes];
        for (int node = 0; node < numNodes; node++) {
            partitioners[node] = new MatrixByRowPartitioner(node, numNodes, rows, rows);
            System.out.println(node + " -> " + partitioners[node].getPartitionShape());
        }
        for (int row = 0; row < rows; row++) {
            int[] partitions = new int[numNodes];
            int firstPartition = -1;
            for (int node = 0; node < numNodes; node++) {
                partitions[node] = partitioners[node].getPartitionOfEntry(dummyEntry.set(row, -1, Double.NaN));
                partitionShapes[node] = partitioners[node].getPartitionShape();
                if(node == 0) {
                    firstPartition = partitions[node];
                }
                Matrix.PartitionShape shape = partitionShapes[node];
                if(partitions[node] == node && (row >= shape.rows + shape.rowOffset || row < shape.rowOffset)) {
                    System.out.println("FUCKED UP! node: " + node + ", row: "+ row);
                    System.out.println("row does not fit in partition: " + shape);
                }
            }
            System.out.println("row " + row + " -> " + Arrays.toString(partitions));
            for (int node = 0; node < numNodes; node++) {
                if(partitions[node] != firstPartition) {
                    System.out.println("FUCKED UP");
                    System.out.println(Arrays.toString(partitions));
                }
            }
        }
    }

}
