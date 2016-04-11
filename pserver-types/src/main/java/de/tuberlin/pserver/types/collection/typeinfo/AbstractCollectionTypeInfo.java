package de.tuberlin.pserver.types.collection.typeinfo;


import de.tuberlin.pserver.types.typeinfo.AbstractDistributedTypeInfo;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

public abstract class AbstractCollectionTypeInfo extends AbstractDistributedTypeInfo implements CollectionTypeInfo {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    // TODO!

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public AbstractCollectionTypeInfo() {}

    public AbstractCollectionTypeInfo(int nodeID, int[] nodes, Class<?> type, String name, DistScheme distScheme) {
        super(nodeID, nodes, type, name, distScheme);

        // TODO!
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    // TODO!
}
