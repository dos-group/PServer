package de.tuberlin.pserver.compiler;

import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
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

    public final FileFormat fileFormat;

    public final MatrixFormat matrixFormat;

    public final String path;

    public final String label;

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
                           final MatrixFormat matrixFormat,
                           final FileFormat fileFormat,
                           final String path,
                           final String label) {

        this.stateName      = stateName;
        this.stateType      = stateType;
        this.elementType    = ElementType.getElementTypeFromClass(stateType);
        this.scope          = scope;
        this.atNodes        = atNodes;
        this.partitioner    = partitioner;
        this.rows           = rows;
        this.cols           = cols;
        this.matrixFormat   = matrixFormat;
        this.fileFormat     = fileFormat;
        this.path           = path;
        this.label          = label;
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
                state.matrixFormat(),
                state.fileFormat(),
                state.path(),
                state.labels()
        );
    }
}
