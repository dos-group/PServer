package de.tuberlin.pserver.crdt;

import java.util.Collection;

// TODO: Do I want to extend java collection? / java Set etc.
// TODO: Auxiliary functions such as for each etc.
public interface CRDT<T> {
    int END = -1;
}