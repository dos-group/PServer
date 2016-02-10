package de.tuberlin.pserver.compiler;

import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.types.matrix.ElementType;
import de.tuberlin.pserver.types.matrix.MatrixFormat;
import de.tuberlin.pserver.types.matrix.f32.Matrix32F;
import de.tuberlin.pserver.types.matrix.f32.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.f32.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.matrix.f32.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.matrix.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.types.matrix.partitioner.PartitionType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public final class StateDescriptor {

    // ---------------------------------------------------
    // State Types.
    // ---------------------------------------------------

    private static final Map<Class<?>, MatrixFormat> MTX_TYPE_FORMAT_MAP;

    static {

        MTX_TYPE_FORMAT_MAP = new HashMap<>();
        MTX_TYPE_FORMAT_MAP.put(Matrix32F.class,         MatrixFormat.DENSE_FORMAT);
        MTX_TYPE_FORMAT_MAP.put(DenseMatrix32F.class,    MatrixFormat.DENSE_FORMAT);
        MTX_TYPE_FORMAT_MAP.put(SparseMatrix32F.class,   MatrixFormat.SPARSE_FORMAT);
        MTX_TYPE_FORMAT_MAP.put(CSRMatrix32F.class,      MatrixFormat.CSR_FORMAT);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String stateName;

    public final Class<?>  stateType;

    public final Scope scope;

    public final int[] atNodes;

    public final PartitionType partitionType;

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
                           final PartitionType partitionType,
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
        this.partitionType = partitionType;
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
        MatrixFormat matrixFormat = MTX_TYPE_FORMAT_MAP.get(field.getType());
        return new StateDescriptor(
                field.getName(),
                field.getType(),
                state.scope(), parsedAtNodes.length > 0 ? parsedAtNodes : fallBackAtNodes,
                state.partitioner(),
                matrixFormat,
                state.rows(),
                state.cols(),
                state.fileFormat(),
                state.path(),
                state.labels()
        );
    }

    public static MatrixPartitioner createMatrixPartitioner(ProgramContext programContext, StateDescriptor state) {
        return MatrixPartitioner.createPartitioner(
                state.partitionType,
                programContext.nodeID,
                state.atNodes,
                state.rows,
                state.cols
        );
    }
}
