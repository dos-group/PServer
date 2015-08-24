package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.partitioning.mtxentries.MatrixEntry;

public class MatrixByRowPartitioner implements IMatrixPartitioner {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int nodeID;

    private final int numNodes;

    private final long rows;

    private final long cols;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixByRowPartitioner(int nodeID, int numNodes, long rows, long cols) {
        this.nodeID   = nodeID;
        this.numNodes = numNodes;
        this.rows     = rows;
        this.cols     = cols;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int getPartitionOfEntry(final MatrixEntry entry) {
        final long pos = entry.getRow() * cols + entry.getCol();
        final long o = (int)((rows / numNodes) * cols);
        int partition = 0;
        for (int p = 0; p < numNodes; ++p) {
            final long n = ((p == numNodes - 1)
                    ? ((rows / numNodes) + rows % numNodes)
                    : (rows / numNodes)) * cols;
            if (pos >= p * o && pos < p * o + n) {
                partition = p;
                break;
            }
        }
        return partition;
    }

    @Override
    public Matrix.PartitionShape getPartitionShape() {
        if(nodeID + 1 >= numNodes)
            return new Matrix.PartitionShape(rows / numNodes + rows % numNodes, cols);
        return new Matrix.PartitionShape(rows / numNodes, cols);
    }
}
