package de.tuberlin.pserver.dsl.unit;


import com.google.common.base.Preconditions;

import java.lang.reflect.Method;

public class UnitDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Method method;

    public final int[] atNodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public UnitDeclaration(final Method method, final int[] atNodes) {

        this.method  = Preconditions.checkNotNull(method);

        this.atNodes = Preconditions.checkNotNull(atNodes);
    }
}
