package de.tuberlin.pserver.runtime.driver;


import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBase;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObject;
import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObjectProxy;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.state.MatrixBuilder;
import de.tuberlin.pserver.runtime.state.MatrixLoader;
import de.tuberlin.pserver.runtime.state.types.DistributedMatrix32F;
import de.tuberlin.pserver.runtime.state.types.DistributedMatrix64F;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

public class StateAllocator {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MatrixLoader matrixLoader;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public StateAllocator(final NetManager netManager,
                          final FileSystemManager fileManager,
                          final RuntimeManager runtimeManager) {

        this.matrixLoader = new MatrixLoader(netManager, fileManager, runtimeManager);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public Pair<MatrixBase, MatrixBase> alloc(final ProgramContext programContext, final StateDescriptor state) {

        MatrixBase stateObj = null;
        MatrixBase proxyObj = null;

        switch (state.scope) {

            case SINGLETON: {
                if (ArrayUtils.contains(state.atNodes, programContext.nodeID)) {

                    stateObj = new MatrixBuilder()
                            .dimension(state.rows, state.cols)
                            .matrixFormat(state.matrixFormat)
                            .elementType(ElementType.getElementTypeFromClass(state.stateType))
                            .build();

                } else {

                    try {

                        MachineDescriptor md = programContext.runtimeContext.infraManager.getMachine(state.atNodes[0]);
                        proxyObj = GlobalObjectProxy.create(state.stateName, programContext.runtimeContext.netManager, md, state.stateType);

                    } catch(Throwable t) {
                        throw new IllegalStateException(t);
                    }
                }
            }
            break;

            case REPLICATED: {
                if (ArrayUtils.contains(state.atNodes, programContext.nodeID)) {
                    stateObj = new MatrixBuilder()
                            .dimension(state.rows, state.cols)
                            .matrixFormat(state.matrixFormat)
                            .elementType(ElementType.getElementTypeFromClass(state.stateType))
                            .build();

                    new GlobalObject<>(programContext.runtimeContext.netManager, (Matrix)stateObj, state.stateName);

                } else {

                    try {

                        MachineDescriptor md = programContext.runtimeContext.infraManager.getMachine(state.atNodes[0]);
                        proxyObj = GlobalObjectProxy.create(state.stateName, programContext.runtimeContext.netManager, md, state.stateType);

                    } catch(Throwable t) {
                        throw new IllegalStateException(t);
                    }
                }
            }
            break;

            case PARTITIONED: {
                switch (ElementType.getElementTypeFromClass(state.stateType)) {
                    case FLOAT_MATRIX: {
                        if (ArrayUtils.contains(state.atNodes, programContext.nodeID)) {
                            stateObj = new DistributedMatrix32F(
                                    programContext,
                                    state.rows,
                                    state.cols,
                                    state.matrixFormat,
                                    state.atNodes,
                                    state.partitioner
                            );
                        }
                    }
                    break;
                    case DOUBLE_MATRIX: {
                        if (ArrayUtils.contains(state.atNodes, programContext.nodeID)) {
                            stateObj = new DistributedMatrix64F(
                                    programContext,
                                    state.rows,
                                    state.cols,
                                    state.matrixFormat,
                                    state.atNodes,
                                    state.partitioner
                            );
                        }
                    }
                    break;
                }
            }
            break;

            case LOGICALLY_PARTITIONED:
                switch (ElementType.getElementTypeFromClass(state.stateType)) {
                    case FLOAT_MATRIX: {
                        stateObj = new DistributedMatrix32F(
                                programContext,
                                state.rows,
                                state.cols,
                                state.matrixFormat,
                                state.atNodes,
                                state.partitioner // TODO: CHANGE!
                        );
                    }
                    break;
                    case DOUBLE_MATRIX: {
                        stateObj = new DistributedMatrix64F(
                                programContext,
                                state.rows,
                                state.cols,
                                state.matrixFormat,
                                state.atNodes,
                                state.partitioner // TODO: CHANGE!
                        );
                    }
                    break;
                }
                break;
        }

        if (stateObj != null) {
            if (!("".equals(state.path))) {
                matrixLoader.addLoadingTask(programContext, state, stateObj);
            }
        } else
            if (proxyObj == null)
                throw new IllegalStateException();

        return Pair.of(stateObj, proxyObj);
    }

    public void loadData(final ProgramContext programContext) throws Exception {
        programContext.synchronizeUnit(UnitMng.GLOBAL_BARRIER);
        matrixLoader.loadFilesIntoDHT();
    }

    public void clearContext() {
        matrixLoader.clearContext();
    }
}
