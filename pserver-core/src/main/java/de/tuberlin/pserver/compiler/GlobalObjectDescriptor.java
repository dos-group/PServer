package de.tuberlin.pserver.compiler;


import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.state.annotations.GlobalObject;

import java.lang.reflect.Field;

public final class GlobalObjectDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String stateName;

    public final Class<?>  stateType;

    public final Class<?>  stateImpl;

    public final int[] atNodes;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public GlobalObjectDescriptor(final String stateName,
                                  final Class<?>  stateType,
                                  final Class<?>  stateImpl,
                                  final int[] atNodes) {

        this.stateName = stateName;
        this.stateType = stateType;
        this.stateImpl = stateImpl;
        this.atNodes   = atNodes;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static GlobalObjectDescriptor fromAnnotatedField(final GlobalObject state, final Field field, final int[] fallBackAtNodes) {
        int[] parsedAtNodes = ParseUtils.parseNodeRanges(state.at());
        return new GlobalObjectDescriptor(
                field.getName(),
                field.getType(),
                state.impl(),
                parsedAtNodes.length > 0 ? parsedAtNodes : fallBackAtNodes
        );
    }
}
