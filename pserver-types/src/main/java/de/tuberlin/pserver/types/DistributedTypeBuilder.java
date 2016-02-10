package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.metadata.DistributedType;

public abstract class DistributedTypeBuilder<T extends DistributedType> {

    abstract public T build(int nodeID, int[] nodes);

    public T build() { return build(-1, null); }
}
