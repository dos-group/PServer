package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.metadata.DistributedType;

public abstract class DistributedTypeBuilder<T extends DistributedType, E extends DistributedDeclaration> {

    abstract public T build(int nodeID, E declaration);

    abstract public T build(int nodeID, int[] nodes);

    public T build() { return build(-1, (int[])null); }
}
