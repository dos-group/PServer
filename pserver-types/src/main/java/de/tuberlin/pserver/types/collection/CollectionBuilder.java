package de.tuberlin.pserver.types.collection;


import de.tuberlin.pserver.types.DistributedTypeBuilder;
import de.tuberlin.pserver.types.collection.annotation.CollectionDeclaration;
import de.tuberlin.pserver.types.collection.implementation.DistributedCollection;

public class CollectionBuilder extends DistributedTypeBuilder<DistributedCollection, CollectionDeclaration> {

    @Override
    public DistributedCollection build(int nodeID, CollectionDeclaration declaration) {
        return null;
    }

    @Override
    public DistributedCollection build(int nodeID, int[] nodes) {
        return null;
    }
}
