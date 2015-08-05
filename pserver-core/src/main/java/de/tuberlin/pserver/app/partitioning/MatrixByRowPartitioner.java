package de.tuberlin.pserver.app.partitioning;

import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.stuff.Utils;

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
