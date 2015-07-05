package de.tuberlin.pserver.app.partitioning;

import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.stuff.Utils;

public class MatrixByRowParitioner implements IMatrixPartitioner {

    private final int instanceId;
    private final int numNodes;
    private final long rows;
    private final long cols;

    public MatrixByRowParitioner(int instanceId, int numNodes, long rows, long cols) {
        this.instanceId = instanceId;
        this.numNodes = numNodes;
        this.rows = rows;
        this.cols = cols;
    }

    @Override
    public int getPartitionOfEntry(MatrixEntry entry) {
        double numOfRowsPerInstance = (double) rows / numNodes;
        double partition = entry.getRow() / numOfRowsPerInstance;
        return Utils.toInt((long) (partition % numNodes));
    }

    @Override
    public Matrix.PartitionShape getPartitionShape() {
        if(instanceId + 1 >= numNodes) {
            return new Matrix.PartitionShape(rows / numNodes + rows % numNodes, cols);
        }
        return new Matrix.PartitionShape(rows / numNodes, cols);
    }

}
