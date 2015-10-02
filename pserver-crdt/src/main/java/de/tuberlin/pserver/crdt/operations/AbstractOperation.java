package de.tuberlin.pserver.crdt.operations;

import java.util.Date;

public class AbstractOperation<T> implements Operation<T> {
    private final int type;
    private final T value;

    public AbstractOperation(int type, T value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public int getType() {
        return this.type;
    }

    @Override
    public T getValue() {
        return this.value;
    }

    // TODO: what about the date variable, is it needed in all register operations?
    public static class RegisterOperation<T> extends AbstractOperation<T> {
        private final Date date;

        public RegisterOperation(int type, T value, Date date) {
            super(type, value);
            this.date = date;
        }

        public Date getDate() {
            return this.date;
        }
    }
}
