package de.tuberlin.pserver.crdt.operations;

import de.tuberlin.pserver.crdt.operations.AbstractOperation;

import java.util.Date;
import java.util.UUID;

//TODO: toString operation?
public class SetOperation<T> extends AbstractOperation<T> {
    // TODO: Semantics of "add" to set and "add" value to counter could be confusing...
    private final long date;
    private final UUID id;

    public SetOperation(int type, T value) {
        super(type, value);
        this.date = 0L;
        this.id = null;
    }

    public SetOperation(int type, T value, Date date) {
        super(type, value);
        this.date = date.getTime();
        this.id = null;
    }

    public SetOperation(int type, T value, UUID id) {
        super(type, value);
        this.date = 0L;
        this.id = id;
    }

    public boolean hasTime() {
        return this.date > 0;
    }

    public long getTime() {
        return this.date;
    }

    public boolean hasId() { return this.id != null; }

    public UUID getId() { return this.id; }

}
