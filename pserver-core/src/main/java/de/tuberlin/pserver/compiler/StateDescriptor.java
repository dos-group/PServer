package de.tuberlin.pserver.compiler;


import de.tuberlin.pserver.types.metadata.DistributedDeclaration;
import de.tuberlin.pserver.types.metadata.DistributedType;

public final class StateDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final DistributedDeclaration declaration;

    public final DistributedType instance;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public StateDescriptor(DistributedDeclaration declaration, DistributedType instance) {
        this.declaration = declaration;
        this.instance = instance;
    }
}
