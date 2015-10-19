package de.tuberlin.pserver.compiler;

import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Layout;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordIteratorProducer;
import de.tuberlin.pserver.runtime.state.partitioner.IMatrixPartitioner;

import java.lang.reflect.Field;

public final class StateDescriptor {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String stateName;

    public final Class<?>  stateType;

    public final ElementType elementType;

    public final Scope scope;

    public final int[] atNodes;

    public final Class<? extends IMatrixPartitioner> partitioner;

    public final long rows;

    public final long cols;

    public final Layout layout;

    public final Format format;

    public final Class<? extends IRecordIteratorProducer> recordFormat;

    public final String path;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public StateDescriptor(final String stateName,
                           final Class<?> stateType,
                           final Scope scope,
                           final int[] atNodes,
                           final Class<? extends IMatrixPartitioner> partitioner,
                           final long rows,
                           final long cols,
                           final Layout layout,
                           final Format format,
                           final Class<? extends IRecordIteratorProducer> recordFormat,
                           final String path) {

        this.stateName      = stateName;
        this.stateType      = stateType;
        this.elementType    = ElementType.getElementTypeFromClass(stateType);
        this.scope          = scope;
        this.atNodes        = atNodes;
        this.partitioner    = partitioner;
        this.rows           = rows;
        this.cols           = cols;
        this.layout         = layout;
        this.format         = format;
        this.recordFormat   = recordFormat;
        this.path           = path;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static StateDescriptor fromAnnotatedField(final State state, final Field field, final int[] fallBackAtNodes) {
        int[] parsedAtNodes = ParseUtils.parseNodeRanges(state.at());
        return new StateDescriptor(
                field.getName(),
                field.getType(),
                state.scope(),
                parsedAtNodes.length > 0 ? parsedAtNodes : fallBackAtNodes,
                state.partitioner(),
                state.rows(),
                state.cols(),
                state.layout(),
                state.format(),
                state.recordFormat(),
                state.path()
        );
    }
}
