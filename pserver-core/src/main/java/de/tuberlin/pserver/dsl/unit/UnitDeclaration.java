package de.tuberlin.pserver.dsl.unit;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

import java.lang.reflect.Method;

public final class UnitDeclaration {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Method method;

    public final int[] atNodes;

    public final String name;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public UnitDeclaration(final Method method, final int[] atNodes) {

        this.method  = Preconditions.checkNotNull(method);

        this.atNodes = Preconditions.checkNotNull(atNodes);

        this.name    = method.getName();
    }

    public static UnitDeclaration fromAnnotatedMethod(Method method, Unit unit) {

        if (method.getReturnType() != void.class)
            throw new IllegalStateException();

        if (method.getParameterTypes().length != 1)
            throw new IllegalStateException();

        if (method.getParameterTypes()[0] != Lifecycle.class)
            throw new IllegalStateException();

        final int[] executingNodeIDs = ParseUtils.parseNodeRanges(unit.at());

        return new UnitDeclaration(
                method,
                executingNodeIDs
        );

    }

}
