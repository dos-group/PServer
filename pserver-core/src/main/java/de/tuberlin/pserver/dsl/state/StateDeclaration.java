package de.tuberlin.pserver.dsl.state;

import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.types.PartitionType;

public final class StateDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String name;

    public final Class<?>  stateType;

    public final LocalScope localScope;

    public final GlobalScope globalScope;

    public final PartitionType partitionType;

    public final long rows;

    public final long cols;

    public final Layout layout;

    public final Format format;

    public final String path;

    public final RemoteUpdate remoteUpdate;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public StateDeclaration(final String name,
                            final Class<?> stateType,
                            final LocalScope localScope,
                            final GlobalScope globalScope,
                            final PartitionType partitionType,
                            final long rows,
                            final long cols,
                            final Layout layout,
                            final Format format,
                            final String path,
                            final RemoteUpdate remoteUpdate) {

        this.name           = name;
        this.stateType      = stateType;
        this.localScope     = localScope;
        this.globalScope    = globalScope;
        this.partitionType  = partitionType;
        this.rows           = rows;
        this.cols           = cols;
        this.layout         = layout;
        this.format         = format;
        this.path           = path;
        this.remoteUpdate   = remoteUpdate;
    }
}
