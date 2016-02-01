package de.tuberlin.pserver.runtime.state.collection;


import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.ArrayList;
import java.util.List;

// TODO: READ IT - http://atomix.io/atomix/user-manual/resources/

public final class DistributedCollection<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final ProgramContext programContext;

    private final List<T> data;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedCollection(final ProgramContext programContext) {

        this.programContext = programContext;

        this.data = new ArrayList<>();
    }
}
