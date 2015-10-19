package de.tuberlin.pserver.runtime;


import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.MatrixBase;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.partitioning.MatrixLoader;
import de.tuberlin.pserver.types.DistributedMatrix32F;
import de.tuberlin.pserver.types.DistributedMatrix64F;
import de.tuberlin.pserver.utils.MatrixBuilder;
import org.apache.commons.lang3.ArrayUtils;

public class StateAllocator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MatrixLoader matrixLoader;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public StateAllocator(final NetManager netManager, final FileSystemManager fileManager) {
        this.matrixLoader = new MatrixLoader(netManager, fileManager);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public MatrixBase alloc(final ProgramContext programContext, final StateDescriptor state) {
        MatrixBase m = null;
        if (ArrayUtils.contains(state.atNodes, programContext.runtimeContext.nodeID)) {
            switch (state.scope) {
                case SINGLETON:
                    m = new MatrixBuilder()
                            .dimension(state.rows, state.cols)
                            .format(state.format)
                            .layout(state.layout)
                            .elementType(ElementType.getElementTypeFromClass(state.stateType))
                            .build();
                    break;
                case REPLICATED:
                    m = new MatrixBuilder()
                            .dimension(state.rows, state.cols)
                            .format(state.format)
                            .layout(state.layout)
                            .elementType(ElementType.getElementTypeFromClass(state.stateType))
                            .build();
                    break;
            }
            switch (state.scope) {
                case PARTITIONED:
                    switch (ElementType.getElementTypeFromClass(state.stateType)) {
                        case FLOAT_MATRIX:
                            m = new DistributedMatrix32F(
                                    programContext,
                                    state.rows,
                                    state.cols,
                                    state.layout,
                                    state.format,
                                    state.atNodes,
                                    state.partitioner
                            );
                            break;
                        case DOUBLE_MATRIX:
                            m = new DistributedMatrix64F(
                                    programContext,
                                    state.rows,
                                    state.cols,
                                    state.layout,
                                    state.format,
                                    state.atNodes,
                                    state.partitioner
                            );
                            break;
                    }
                    break;
                case LOGICALLY_PARTITIONED:
                    switch (ElementType.getElementTypeFromClass(state.stateType)) {
                        case FLOAT_MATRIX:
                            m =  new DistributedMatrix32F(
                                    programContext,
                                    state.rows,
                                    state.cols,
                                    state.layout,
                                    state.format,
                                    state.atNodes,
                                    state.partitioner // TODO: CHANGE!
                            );
                            break;
                        case DOUBLE_MATRIX:
                            m = new DistributedMatrix64F(
                                    programContext,
                                    state.rows,
                                    state.cols,
                                    state.layout,
                                    state.format,
                                    state.atNodes,
                                    state.partitioner // TODO: CHANGE!
                            );
                            break;
                    }
                    break;
            }
        }

        if (m != null) {
            if (!("".equals(state.path))) {
                matrixLoader.addLoadingTask(programContext, state, m);
            }
        } else
            throw new IllegalStateException();

        return m;
    }

    public void loadData(final ProgramContext programContext) throws Exception {
        programContext.synchronizeUnit(UnitMng.GLOBAL_BARRIER);
        matrixLoader.loadFilesIntoDHT();
    }

    public void clearContext() {
        matrixLoader.clearContext();
    }


    /*private MatrixBase allocSingletonState(final ProgramContext programContext, final StateDescriptor state) {
        if (state.atNodes.length != 1)
            throw new IllegalStateException("State object can only exist at a single node.");
        if (programContext.runtimeContext.nodeID == state.atNodes[0]) {
            return new MatrixBuilder()
                    .dimension(state.rows, state.cols)
                    .format(state.format)
                    .layout(state.layout)
                    .elementType(ElementType.getElementTypeFromClass(state.stateType))
                    .build();
        }
        return null;
    }*/


    /*private MatrixBase allocSingletonMatrix(final ProgramContext programContext, final StateDescriptor decl) {
        if (decl.atNodes.length != 1)
            throw new IllegalStateException();
        if (decl.atNodes[0] < 0 || decl.atNodes[0] > programContext.runtimeContext.numOfNodes - 1)
            throw new IllegalStateException();
        if (programContext.runtimeContext.nodeID == decl.atNodes[0]) {
            final MatrixBase so = new MatrixBuilder()
                    .dimension(decl.rows, decl.cols)
                    .format(decl.format)
                    .layout(decl.layout)
                    .elementType(decl.stateType)
                    .build();
            new RemoteMatrixStub(programContext, decl.stateName, (Matrix)so);
            return so;
        } else {
            return new RemoteMatrixSkeleton(
                    programContext,
                    decl.stateName,
                    decl.atNodes[0],
                    decl.rows,
                    decl.cols,
                    decl.format,
                    decl.layout
            );
        }
    }*/
}
