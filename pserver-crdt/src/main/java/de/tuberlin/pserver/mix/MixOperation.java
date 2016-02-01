package de.tuberlin.pserver.mix;

import de.tuberlin.pserver.crdt.operations.Operation;

public class MixOperation<T> implements Operation<T> {
    private int sessionID;
    private T value;
    private MixOperationType type = MixOperation.MixOperationType.AVG;
    public enum MixOperationType {
        AVG
    }

    public MixOperation(int sessionID, T value) {
        this.sessionID = sessionID;
        this.value = value;
    }

    public int getSessionID() {
        return sessionID;
    }

    public MixOperation.MixOperationType getOpType() {
        return type;
    }

    @Override
    public OpType getType() {
        return null;
    }

    @Override
    public T getValue() {
        return value;
    }
}
