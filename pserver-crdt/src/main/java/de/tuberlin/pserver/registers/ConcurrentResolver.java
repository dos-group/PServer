package de.tuberlin.pserver.registers;

public interface ConcurrentResolver<T> {

    boolean resolveConcurrent(T op1, T op2);

}
