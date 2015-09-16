package de.tuberlin.pserver.crdt;


import de.tuberlin.pserver.runtime.DataManager;

public interface CRDT {
    int END = -1;
    int ADD = 0;
    int SUBTRACT = 1;

    //void applyUpdate(int srcNodeID, Operation op, DataManager dm);
    //void broadcast(int op, int value, DataManager dm);

    // Signal the end of processing on this crdt and that all replicas should wait for "finished" signal before ending
    // public void consolidate();
}