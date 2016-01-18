package de.tuberlin.pserver.runtime.driver;


import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBase;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObject;
import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObjectProxy;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.state.MatrixBuilder;
import de.tuberlin.pserver.runtime.state.MatrixLoader;
import de.tuberlin.pserver.runtime.state.cache.MatrixCache;
import de.tuberlin.pserver.runtime.state.cache.MatrixCacheProvider;
import de.tuberlin.pserver.runtime.state.types.DistributedMatrix32F;
import de.tuberlin.pserver.runtime.state.types.DistributedMatrix64F;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

public class StateAllocator {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final int DEFAULT_CACHE_SIZE = 1024;

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

    @SuppressWarnings("unchecked")
    public Pair<MatrixBase, MatrixBase> alloc(final ProgramContext programContext, final StateDescriptor state) {

        //try {

            MatrixBase stateObj = null;

            MatrixBase proxyObj = null;

            switch (state.scope) {

                case SINGLETON: {
                    if (ArrayUtils.contains(state.atNodes, programContext.nodeID)) {

                        stateObj = new MatrixBuilder()
                                .dimension(state.rows, state.cols)
                                .format(state.format)
                                .elementType(ElementType.getElementTypeFromClass(state.stateType))
                                .build();

                        //new MatrixCacheProvider<>(
                        //        programContext.runtimeContext,
                        //        (Matrix) stateObj
                        //);

                    } else {

                        /*final MatrixBase cache = new MatrixBuilder()
                                .dimension(state.rows, state.cols)
                                .format(Format.SPARSE_FORMAT)
                                .elementType(ElementType.getElementTypeFromClass(state.stateType))
                                .build();

                        new MatrixCache(
                                programContext.runtimeContext,
                                state.atNodes,
                                null,
                                (Matrix) cache,
                                DEFAULT_CACHE_SIZE
                        );*/
                    }
                }
                break;

                case REPLICATED: {
                    if (ArrayUtils.contains(state.atNodes, programContext.nodeID)) {
                        stateObj = new MatrixBuilder()
                                .dimension(state.rows, state.cols)
                                .format(state.format)
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
                                        state.format,
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
                                        state.format,
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
                                    state.format,
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
                                    state.format,
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
