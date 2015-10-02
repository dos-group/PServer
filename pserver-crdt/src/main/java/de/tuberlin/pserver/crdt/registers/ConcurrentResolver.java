package de.tuberlin.pserver.crdt.registers;

public interface ConcurrentResolver<T> {

    boolean resolveConcurrent(T op1, T op2);

}
