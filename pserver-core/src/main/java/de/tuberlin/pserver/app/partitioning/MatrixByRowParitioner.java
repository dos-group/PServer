package de.tuberlin.pserver.app.partitioning;

import de.tuberlin.pserver.app.types.MatrixEntry;
import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.math.stuff.Utils;

public class MatrixByRowParitioner implements IMatrixPartitioner {

    private final int instanceId;
    private final int numNodes;

    public MatrixByRowParitioner(int instanceId, int numNodes) {
        this.instanceId = instanceId;
        this.numNodes = numNodes;
    }

    @Override
    public int getPartitionOfEntry(MatrixEntry entry) {
        return Utils.toInt(entry.getRow() % numNodes);
    }

    @Override
    public Matrix.Dimension getPartitionedDimension(long rows, long cols) {
        if(instanceId + 1 >= numNodes) {
            return new Matrix.Dimension(rows / numNodes + rows % numNodes, cols);
        }
        return new Matrix.Dimension(rows / numNodes, cols);
    }

}
