package de.tuberlin.pserver.types.typeinfo;

import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;
import de.tuberlin.pserver.types.typeinfo.properties.InputDescriptor;
import de.tuberlin.pserver.types.typeinfo.properties.InternalData;

import java.io.Serializable;

public interface DistributedTypeInfo extends Serializable {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    int nodeId();

    int[] nodes();

    // ---------------------------------------------------

    DistScheme distributionScheme();

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

    // ---------------------------------------------------

    String name();

    Class<?> type();

    // ---------------------------------------------------

    void input(InputDescriptor id);

    InputDescriptor input();
}
