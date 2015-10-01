package de.tuberlin.pserver.registers;

import de.tuberlin.pserver.crdt.AbstractOperation;

import java.util.Date;

// TODO: what about the date variable, is it needed in all register operations?
public class RegisterOperation<T> extends AbstractOperation<T> {
    public static final int WRITE = 1;
    private final Date date;

    public RegisterOperation(int type, T value, Date date) {
        super(type, value);
        this.date = date;
    }

    public Date getDate() {
        return this.date;
    }
}
