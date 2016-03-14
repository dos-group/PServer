package de.tuberlin.pserver.radt;

public interface Cemetery<T extends CObject> {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    boolean enrol(int nodeId, T cObj);

    boolean withdraw(T cObj);

    boolean purge();

}
