package de.tuberlin.pserver.types;

import de.tuberlin.pserver.types.metadata.DistributionScheme;

public abstract class DistributedDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final int[] nodes;

    public final DistributionScheme distributionScheme;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedDeclaration(int[] nodes, DistributionScheme distributionScheme) {

        this.nodes = nodes;

        this.distributionScheme = distributionScheme;
    }
}
