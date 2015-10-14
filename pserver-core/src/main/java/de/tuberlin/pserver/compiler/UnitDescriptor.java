package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

import java.lang.reflect.Method;

public final class UnitDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final Method method;

    public final String unitName;

    public int[] atNodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public UnitDescriptor(final Method method, final int[] atNodes) {

        this.method  = Preconditions.checkNotNull(method);

        this.unitName = method.getName();

        this.atNodes = Preconditions.checkNotNull(atNodes);
    }

    // ---------------------------------------------------
    // Public Method.
    // ---------------------------------------------------

    public static UnitDescriptor fromAnnotatedMethod(final Method method, final Unit unit) {

        if (method.getReturnType() != void.class)
            throw new IllegalStateException();

        if (method.getParameterTypes().length != 1)
            throw new IllegalStateException();

        if (method.getParameterTypes()[0] != Lifecycle.class)
            throw new IllegalStateException();

        final int[] executingNodeIDs = ParseUtils.parseNodeRanges(unit.at());

        return new UnitDescriptor(
                method,
                executingNodeIDs
        );
    }
}
