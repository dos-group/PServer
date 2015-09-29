package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.AbstractOperation;
import de.tuberlin.pserver.crdt.Operation;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

//TODO: toString operation?
public class SetOperation<T> extends AbstractOperation<T> {
    // TODO: Semantics of "add" to set and "add" value to counter could be confusing...
    public static final int ADD = 1;
    public static final int REMOVE = 2;
    private final long date;

    public SetOperation(int type, T value) {
        super(type, value);
        this.date = 0L;
    }

    public SetOperation(int type, T value, Date date) {
        super(type, value);
        this.date = date.getTime();
    }

    public long getTime() {
        return this.date;
    }

    public boolean hasTime() {
        return this.date > 0;
    }
}
