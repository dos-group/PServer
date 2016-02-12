package de.tuberlin.pserver.types.collection;


import de.tuberlin.pserver.types.AbstractBuilder;
import de.tuberlin.pserver.types.collection.annotations.Collection;
import de.tuberlin.pserver.types.collection.typeinfo.CollectionTypeInfo;

public class CollectionBuilder extends AbstractBuilder<CollectionTypeInfo, Collection> {

    @Override
    public CollectionTypeInfo build(int nodeID, int[] allNodes, Class<?> type, String name, Collection annotation) {
        return null;
    }

    @Override
    public CollectionTypeInfo build(int nodeID, int[] nodes, Class<?> type, String name) {
        return null;
    }
}
