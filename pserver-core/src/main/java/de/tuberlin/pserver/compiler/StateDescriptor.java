package de.tuberlin.pserver.compiler;


import de.tuberlin.pserver.types.DistributedDeclaration;
import de.tuberlin.pserver.types.metadata.DistributedType;

public final class StateDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String name;

    public final Class<?>  type;

    public final DistributedDeclaration declaration;

    public final DistributedType instance;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public StateDescriptor(String name, Class<?>  type, DistributedDeclaration declaration, DistributedType instance) {
        this.name = name;
        this.type = type;
        this.declaration = declaration;
        this.instance = instance;
    }
}
