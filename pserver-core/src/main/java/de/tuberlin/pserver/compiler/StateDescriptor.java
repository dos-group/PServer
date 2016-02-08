package de.tuberlin.pserver.compiler;

import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.PartitionerType;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.math.matrix.MatrixFormat;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix64F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix32F;
import de.tuberlin.pserver.math.matrix.sparse.SparseMatrix64F;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.runtime.state.matrix.disttypes.DistributedMatrix32F;
import de.tuberlin.pserver.runtime.state.matrix.disttypes.DistributedMatrix64F;
import de.tuberlin.pserver.runtime.state.matrix.partitioner.MatrixPartitioner;

import java.lang.reflect.Field;

public final class StateDescriptor {

    // ---------------------------------------------------
    // State Types.
    // ---------------------------------------------------

    public static final Class<?>[] LOCAL_STATE_TYPES = {

            Matrix32F.class,
            Matrix64F.class,
            DenseMatrix32F.class,
            DenseMatrix64F.class,
            SparseMatrix32F.class,
            SparseMatrix64F.class,
            DistributedMatrix32F.class,
            DistributedMatrix64F.class,
    };

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String stateName;

    public final Class<?>  stateType;

    public final Scope scope;

    public final int[] atNodes;

    public final PartitionerType partitionerType;

    // ---------------------------------------------------

    public final MatrixFormat matrixFormat;

    public final ElementType elementType;

    public final long rows;

    public final long cols;

    // ---------------------------------------------------

    public final FileFormat fileFormat;

    public final String path;

    public final String label;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public StateDescriptor(final String stateName,
                           final Class<?> stateType,
                           final Scope scope,
                           final int[] atNodes,
                           final PartitionerType partitionerType,
                           final MatrixFormat matrixFormat,
                           final long rows,
                           final long cols,
                           final FileFormat fileFormat,
                           final String path,
                           final String label) {

        this.stateName      = stateName;
        this.stateType      = stateType;
        this.scope          = scope;
        this.atNodes        = atNodes;
        this.partitionerType = partitionerType;
        this.matrixFormat   = matrixFormat;
        this.rows           = rows;
        this.cols           = cols;
        this.fileFormat     = fileFormat;
        this.path           = path;
        this.label          = label;

        this.elementType    = ElementType.getElementTypeFromClass(stateType);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static StateDescriptor fromAnnotatedField(final State state, final Field field, final int[] fallBackAtNodes) {
        int[] parsedAtNodes = ParseUtils.parseNodeRanges(state.at());
        return new StateDescriptor(
                field.getName(),
                field.getType(),
                state.scope(), parsedAtNodes.length > 0 ? parsedAtNodes : fallBackAtNodes,
                state.partitioner(),
                state.matrixFormat(), state.rows(),
                state.cols(),
                state.fileFormat(),
                state.path(),
                state.labels()
        );
    }

    public static MatrixPartitioner createMatrixPartitioner(ProgramContext programContext, StateDescriptor state) {
        return MatrixPartitioner.create(
                state.partitionerType,
                state.rows, state.cols,
                programContext.nodeID,
                state.atNodes
        );
    }
}
