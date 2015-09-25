package de.tuberlin.pserver.sets;

import de.tuberlin.pserver.crdt.Operation;

import java.util.HashSet;
import java.util.Set;

public class SetOperation<T> extends Operation {
    // TODO: Semantics of "add" to set and "add" value to counter could be confusing...
    public static final int ADD = 1;
    private Set<T> value;

    public SetOperation(int type, Set<T> value) {
        super(type);
        this.value = value;
    }

    public SetOperation(int type, T value) {
        super(type);
        this.value = new HashSet<T>();
        this.value.add(value);
    }

    public Set<T> getValue() {
        return this.value;
    }
}
