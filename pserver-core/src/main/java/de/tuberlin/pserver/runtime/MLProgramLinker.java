package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.state.StateExtractor;
import de.tuberlin.pserver.dsl.state.StateMerger;
import de.tuberlin.pserver.dsl.state.SharedState;
import de.tuberlin.pserver.dsl.state.StateDeclaration;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
import de.tuberlin.pserver.runtime.state.controller.MatrixDeltaMergeUpdateController;
import de.tuberlin.pserver.runtime.state.controller.MatrixMergeUpdateController;
import de.tuberlin.pserver.runtime.state.controller.RemoteUpdateController;
import de.tuberlin.pserver.runtime.state.controller.VectorMergeUpdateController;
import de.tuberlin.pserver.runtime.state.filter.MatrixUpdateFilter;
import de.tuberlin.pserver.runtime.state.merger.UpdateMerger;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.PartitionType;
import de.tuberlin.pserver.types.RemoteMatrixSkeleton;
import de.tuberlin.pserver.types.RemoteMatrixStub;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public final class MLProgramLinker {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(MLProgramLinker.class);

    private final MLProgramContext programContext;

    private final Class<? extends MLProgram> programClass;

    private List<StateDeclaration> stateDecls;

    private final DataManager dataManager;

    private final RuntimeContext runtimeContext;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MLProgramLinker(final MLProgramContext programContext,
                           final Class<? extends MLProgram> programClass) {

        this.programContext   = Preconditions.checkNotNull(programContext);
        this.programClass     = Preconditions.checkNotNull(programClass);
        this.dataManager      = programContext.runtimeContext.dataManager;
        this.runtimeContext   = programContext.runtimeContext;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void link(final SlotContext slotContext, final MLProgram instance) throws Exception {

        //slotContext.CF.parScope().slot(0).exe(() -> {

        if (slotContext.slotID == 0) {

            stateDecls = new ArrayList<>();

            final String slotIDStr = "[" + runtimeContext.nodeID
                    + " | " + slotContext.slotID + "] ";

            LOG.info(slotIDStr + "Enter " + slotContext.programContext.simpleClassName + " linking phase.");

            final long start = System.currentTimeMillis();

            analyzeStateAnnotations();

            allocateStateObjects(slotContext);

            analyzeAndWireDeltaFilterAnnotations(instance);

            analyzeAndWireDeltaMergerAnnotations(instance);

            dataManager.loadInputData(slotContext);

            for (final StateDeclaration decl : stateDecls) {

                programContext.put(stateDeclarationName(decl.name), decl);
            }

            programContext.put(stateDeclarationListName(), stateDecls);

            Thread.sleep(5000); // TODO: Wait until all objects are placed in DHT and accessible...

            slotContext.programContext.programLoadBarrier.countDown();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + slotContext.programContext.simpleClassName
                    + " loading linking [duration: " + (end - start) + " ms].");
        }

        //});

        slotContext.programContext.programLoadBarrier.await();
    }

    public void fetchStateObjects(final MLProgram program) throws Exception {

        stateDecls = programContext.get(stateDeclarationListName());

        for (final StateDeclaration decl : stateDecls) {

            final Field f = programClass.getDeclaredField(decl.name);

            final Object stateObj = dataManager.getObject(decl.name);

            Preconditions.checkState(stateObj != null);

            f.set(program, stateObj);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void analyzeStateAnnotations() {
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof SharedState) {
                    final SharedState stateProperties = (SharedState) an;
                    final StateDeclaration decl = new StateDeclaration(
                            field.getName(),
                            field.getType(),
                            stateProperties.localScope(),
                            stateProperties.globalScope(),
                            parseIntArray(stateProperties.at()),
                            stateProperties.partitionType(),
                            stateProperties.rows(),
                            stateProperties.cols(),
                            stateProperties.layout(),
                            stateProperties.format(),
                            stateProperties.recordFormat(),
                            stateProperties.path(),
                            stateProperties.remoteUpdate()
                    );
                    stateDecls.add(decl);
                }
            }
        }
    }

    private void analyzeAndWireDeltaFilterAnnotations(final MLProgram instance) throws Exception {
        for (final Field field : Preconditions.checkNotNull(programClass).getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof StateExtractor) {
                    final StateExtractor filterProperties = (StateExtractor) an;
                    StringTokenizer st = new StringTokenizer(filterProperties.stateObjects(), ",");
                    while (st.hasMoreTokens()) {
                        final String stateObjName = st.nextToken().replaceAll("\\s+","");
                        final RemoteUpdateController remoteUpdateController =
                                programContext.get(remoteUpdateControllerName(stateObjName));
                        if (remoteUpdateController == null)
                            throw new IllegalStateException();
                        final MatrixUpdateFilter filter = (MatrixUpdateFilter)field.get(instance);
                        remoteUpdateController.setUpdateFilter(filter);
                    }
                }
            }
        }
    }

    private void analyzeAndWireDeltaMergerAnnotations(final MLProgram instance) throws Exception {
        for (final Field field : Preconditions.checkNotNull(programClass).getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof StateMerger) {
                    final StateMerger mergerProperties = (StateMerger) an;
                    StringTokenizer st = new StringTokenizer(mergerProperties.stateObjects(), ",");
                    while (st.hasMoreTokens()) {
                        final String stateObjName = st.nextToken().replaceAll("\\s+", "");
                        final RemoteUpdateController remoteUpdateController =
                                programContext.get(remoteUpdateControllerName(stateObjName));
                        if (remoteUpdateController == null)
                            throw new IllegalStateException();
                        final UpdateMerger merger = (UpdateMerger) field.get(instance);
                        remoteUpdateController.setUpdateMerger(merger);
                    }
                }
            }
        }
    }

    private void allocateStateObjects(final SlotContext slotContext) throws Exception {
        Preconditions.checkNotNull(stateDecls);
        for (final StateDeclaration decl : stateDecls) {
            if (Matrix.class.isAssignableFrom(decl.stateType)) {
                switch (decl.globalScope) {
                    case SINGLETON: {

                        if (decl.atNodes.length != 1)
                            throw new IllegalStateException();

                        if (decl.atNodes[0] < 0 || decl.atNodes[0] > runtimeContext.numOfNodes - 1)
                            throw new IllegalStateException();

                        if (runtimeContext.nodeID == decl.atNodes[0]) {

                            final SharedObject so = new MatrixBuilder()
                                    .dimension(decl.rows, decl.cols)
                                    .format(decl.format)
                                    .layout(decl.layout)
                                    .build();

                            dataManager.putObject(decl.name, so);

                            new RemoteMatrixStub(slotContext, decl.name, (Matrix)so);

                        } else {

                            final RemoteMatrixSkeleton remoteMatrixSkeleton = new RemoteMatrixSkeleton(
                                    slotContext,
                                    decl.name,
                                    decl.atNodes[0],
                                    decl.rows,
                                    decl.cols,
                                    decl.format,
                                    decl.layout
                            );

                            dataManager.putObject(decl.name, remoteMatrixSkeleton);
                        }

                    } break;
                    case REPLICATED: {
                        if ("".equals(decl.path)) {

                            final SharedObject so = new MatrixBuilder()
                                    .dimension(decl.rows, decl.cols)
                                    .format(decl.format)
                                    .layout(decl.layout)
                                    .build();

                            switch (decl.remoteUpdate) {
                                case NO_UPDATE: break;
                                case SIMPLE_MERGE_UPDATE:
                                        programContext.put(remoteUpdateControllerName(decl.name),
                                                new MatrixMergeUpdateController(slotContext, decl.name, (Matrix)so));
                                    break;
                                case DELTA_MERGE_UPDATE:
                                        programContext.put(remoteUpdateControllerName(decl.name),
                                                new MatrixDeltaMergeUpdateController(slotContext, decl.name, (Matrix)so));
                                    break;
                            }

                            dataManager.putObject(decl.name, so);
                        } else {
                            dataManager.loadAsMatrix(
                                    slotContext,
                                    decl.path,
                                    decl.name,
                                    decl.rows,
                                    decl.cols,
                                    decl.globalScope,
                                    decl.partitionType,
                                    decl.recordFormatConfigClass.newInstance(),
                                    decl.format,
                                    decl.layout
                            );
                        }
                    } break;
                    case PARTITIONED: {
                        if ("".equals(decl.path)) {
                            final SharedObject so = new DistributedMatrix(
                                    slotContext,
                                    decl.rows, decl.cols,
                                    PartitionType.ROW_PARTITIONED,
                                    decl.layout,
                                    decl.format,
                                    false
                            );
                            dataManager.putObject(decl.name, so);
                        } else {
                            dataManager.loadAsMatrix(
                                    slotContext,
                                    decl.path,
                                    decl.name,
                                    decl.rows,
                                    decl.cols,
                                    decl.globalScope,
                                    decl.partitionType,
                                    decl.recordFormatConfigClass.newInstance(),
                                    decl.format,
                                    decl.layout
                            );
                        }
                    } break;
                    case LOGICALLY_PARTITIONED:
                        final SharedObject so = new DistributedMatrix(
                                slotContext,
                                decl.rows, decl.cols,
                                PartitionType.ROW_PARTITIONED,
                                decl.layout,
                                decl.format,
                                true
                        );
                        dataManager.putObject(decl.name, so);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } else if (Vector.class.isAssignableFrom(decl.stateType)) {
                if (!"".equals(decl.path))
                    throw new IllegalStateException();
                switch (decl.globalScope) {
                    case REPLICATED: {
                        final SharedObject so = new VectorBuilder()
                                .dimension(decl.layout == Layout.ROW_LAYOUT ? decl.cols : decl.rows)
                                .format(decl.format)
                                .layout(decl.layout)
                                .build();

                        switch (decl.remoteUpdate) {
                            case NO_UPDATE: break;
                            case SIMPLE_MERGE_UPDATE:
                                programContext.put(remoteUpdateControllerName(decl.name),
                                        new VectorMergeUpdateController(slotContext, decl.name, (Vector)so));
                                break;
                            case DELTA_MERGE_UPDATE:
                                throw new UnsupportedOperationException();
                        }

                        dataManager.putObject(decl.name, so);
                    } break;
                    case PARTITIONED: throw new UnsupportedOperationException();
                    default: throw new UnsupportedOperationException();
                }
            } else
                throw new UnsupportedOperationException();
        }
    }

    private int[] parseIntArray (final String intArrayStr) {
        final StringTokenizer st = new StringTokenizer(Preconditions.checkNotNull(intArrayStr), ",");
        final List<Integer> vals = new ArrayList<>();
        while (st.hasMoreTokens())
            vals.add(Integer.valueOf(st.nextToken().replaceAll("\\s+", "")));
        return ArrayUtils.toPrimitive(vals.toArray(new Integer[vals.size()]));
    }

    // ---------------------------------------------------
    // Utility Methods.
    // ---------------------------------------------------

    public static String stateDeclarationListName() { return "__state_declarations__"; }

    public static String stateDeclarationName(final String name) { return "__state_declaration_" + name; }

    public static String remoteUpdateControllerName(final String name) { return "__remote_update_controller_" + name; }
}
