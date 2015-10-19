package de.tuberlin.pserver.runtime.partitioning;

import de.tuberlin.pserver.math.matrix.Matrix;

import java.io.Serializable;

// simple wrapper for a partition to attach a nodeId to it
public class RemotePartition implements Serializable {

    public final Matrix.PartitionShape shape;

    public final int nodeId;

    public RemotePartition(Matrix.PartitionShape shape, int nodeId) {
        this.shape = shape;
        this.nodeId = nodeId;
    }
}
