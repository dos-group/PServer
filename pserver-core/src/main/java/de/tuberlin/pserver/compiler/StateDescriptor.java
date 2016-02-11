package de.tuberlin.pserver.compiler;

import de.tuberlin.pserver.commons.utils.ParseUtils;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.sparse.CSRMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.f32.sparse.SparseMatrix32F;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.MatrixPartitioner;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.PartitionType;
import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import de.tuberlin.pserver.types.matrix.implementation.properties.MatrixType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public final class StateDescriptor {

    // ---------------------------------------------------
    // State Types.
    // ---------------------------------------------------

    private static final Map<Class<?>, MatrixType> MTX_TYPE_FORMAT_MAP;

    static {

        MTX_TYPE_FORMAT_MAP = new HashMap<>();
        MTX_TYPE_FORMAT_MAP.put(Matrix32F.class,         MatrixType.DENSE_FORMAT);
        MTX_TYPE_FORMAT_MAP.put(DenseMatrix32F.class,    MatrixType.DENSE_FORMAT);
        MTX_TYPE_FORMAT_MAP.put(SparseMatrix32F.class,   MatrixType.SPARSE_FORMAT);
        MTX_TYPE_FORMAT_MAP.put(CSRMatrix32F.class,      MatrixType.CSR_FORMAT);
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

    public final MatrixType matrixType;

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
                           final MatrixType matrixType,
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
        this.matrixType = matrixType;
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
        MatrixType matrixType = MTX_TYPE_FORMAT_MAP.get(field.getType());
        return new StateDescriptor(
                field.getName(),
                field.getType(),
                state.scope(), parsedAtNodes.length > 0 ? parsedAtNodes : fallBackAtNodes,
                state.partitioner(),
                matrixType,
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
