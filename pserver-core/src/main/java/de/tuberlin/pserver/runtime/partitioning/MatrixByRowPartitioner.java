package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.utils.Utils;
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
        this.nodeID = nodeID;
        this.numNodes = numNodes;
        this.rows = rows;
        this.cols = cols;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int getPartitionOfEntry(MatrixEntry entry) {
        double numOfRowsPerInstance = (double) rows / numNodes;
        double partition = entry.getRow() / numOfRowsPerInstance;
        int result = Utils.toInt((long) (partition % numNodes));
        return result;
    }

    @Override
    public Matrix.PartitionShape getPartitionShape() {
        if(nodeID + 1 >= numNodes) {
            return new Matrix.PartitionShape(rows / numNodes + rows % numNodes, cols);
        }
        return new Matrix.PartitionShape(rows / numNodes, cols);
    }

}
