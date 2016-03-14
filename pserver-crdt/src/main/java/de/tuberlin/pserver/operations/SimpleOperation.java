package de.tuberlin.pserver.operations;

import com.google.common.base.Preconditions;

import java.io.Serializable;

public class SimpleOperation<T> implements Operation<T>, Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final OpType type;

    private final T value;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    // No args constructor for Serialization (?)
    public SimpleOperation() {

        this.type = null;

        this.value = null;

    }

    public SimpleOperation(OpType type, T value) {

        Preconditions.checkNotNull(type, "Operation type can not be null");

        this.type = type;

        this.value = value;

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public OpType getType() {

        return this.type;

    }

    @Override
    public T getValue() {

        return this.value;

    }

}
