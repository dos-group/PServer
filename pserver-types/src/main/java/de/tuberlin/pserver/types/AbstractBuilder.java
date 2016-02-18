package de.tuberlin.pserver.types;


import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;

import java.lang.annotation.Annotation;

public abstract class AbstractBuilder<T extends DistributedTypeInfo, A extends Annotation> {

    abstract public T build(int nodeID, int[] allNodes, Class<?> type, String name, A annotation);

    abstract public T build(int nodeID, int[] nodes, Class<?> type, String name);

    public T build() { return build(-1, null, null, null); }
}
