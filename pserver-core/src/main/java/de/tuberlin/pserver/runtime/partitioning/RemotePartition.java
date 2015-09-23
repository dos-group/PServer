package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;

public class RemotePartition {

    public final Matrix.PartitionShape shape;

    public final int nodeId;

    public RemotePartition(Matrix.PartitionShape shape, int nodeId) {
        this.shape = shape;
        this.nodeId = nodeId;
    }
}
