package de.tuberlin.pserver.crdt.radt.hashtable;

import de.tuberlin.pserver.crdt.radt.S4Vector;

import java.util.ArrayList;
import java.util.List;

public class Cemetery<T> {
    public final List<Slot> cemetery;

    public Cemetery() {
        this.cemetery = new ArrayList<>();
    }

    public boolean enroll(Slot slot) {
        slot.setValue(null);
        return cemetery.add(slot);
    }

    public boolean withdraw(Slot slot) {
        return cemetery.remove(slot);
    }

    public boolean purge() {
        // TODO: purge this puppy
        return false;
    }
}
