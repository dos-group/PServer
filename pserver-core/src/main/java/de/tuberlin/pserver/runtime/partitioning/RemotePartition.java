package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;

// simple wrapper for a partition to attach a nodeId to it
public class RemotePartition {

    public final Matrix.PartitionShape shape;

    public final int nodeId;

    public RemotePartition(Matrix.PartitionShape shape, int nodeId) {
        this.shape = shape;
        this.nodeId = nodeId;
    }
}
