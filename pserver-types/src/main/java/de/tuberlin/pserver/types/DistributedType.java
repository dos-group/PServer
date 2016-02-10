package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.matrix.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.types.matrix.partitioner.PartitionShape;

import java.io.Serializable;

public interface DistributedType extends Serializable {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    int nodeId();

    int[] nodes();

    void setOwner(final Object owner);

    Object owner();

    void lock();

    void unlock();

    long sizeOf();

    Object toArray();

    void setArray(final Object data);


    MatrixPartitioner partitioner();

    PartitionShape shape();
}
