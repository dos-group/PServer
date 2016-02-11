package de.tuberlin.pserver.types.metadata;

import java.io.Serializable;

public interface DistributedType extends Serializable {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    int nodeId();

    int[] nodes();

    // ---------------------------------------------------

    void owner(final Object owner);

    Object owner();

    // ---------------------------------------------------

    void lock();

    void unlock();

    // ---------------------------------------------------

    long sizeOf();

    // ---------------------------------------------------

    <T> InternalData<T> internal();
}
