package de.tuberlin.pserver.types.collection;


import java.util.ArrayList;
import java.util.List;

// TODO: READ IT - http://atomix.io/atomix/user-manual/resources/

public final class DistributedCollection<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<T> data;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedCollection() {

        this.data = new ArrayList<>();
    }
}
