package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.runtime.DataManager;

import java.util.Collection;

// TODO: Do I want to extend java collection? / java ISet etc.
// TODO: Auxiliary functions such as for each etc.
public interface CRDT<T> {
    int END = -1;
    int SUM = 1;
    int SUBTRACT = 2;
    int ADD = 3;
    int REMOVE = 4;
    int WRITE = 5;


    void run(DataManager dataManager);
    void finish(DataManager dataManager);
}