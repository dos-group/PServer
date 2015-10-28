package de.tuberlin.pserver.radt.hashtable;

import java.util.ArrayList;
import java.util.List;

// TODO: Cemetery<T extends CObject>
public class Cemetery<T> {
    public final List<T> cemetery;

    public Cemetery() {
        this.cemetery = new ArrayList<>();
    }

    public boolean enrol(T cObj) {
        return cemetery.add(cObj);
    }

    public boolean withdraw(T cObj) {
        return cemetery.remove(cObj);
    }

    public boolean purge() {
        // TODO: purge this puppy
        return false;
    }
}
