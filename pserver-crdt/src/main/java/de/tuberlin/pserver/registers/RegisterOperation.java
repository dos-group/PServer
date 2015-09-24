package de.tuberlin.pserver.registers;

import de.tuberlin.pserver.crdt.Operation;

import java.util.Date;

public class RegisterOperation<T> extends Operation {
    public static final int WRITE = 1;
    Date date;
    T value;

    public RegisterOperation(int type, T value, Date date) {
        super(type);
        this.date = date;
        this.value = value;
    }

    public Date getDate() {
        return this.date;
    }

    public T getValue() {
        return this.value;
    }
}
