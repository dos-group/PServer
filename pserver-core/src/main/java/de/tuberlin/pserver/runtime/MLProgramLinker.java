package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.state.DeltaFilter;
import de.tuberlin.pserver.dsl.state.DeltaMerger;
import de.tuberlin.pserver.dsl.state.SharedState;
import de.tuberlin.pserver.dsl.state.StateDeclaration;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.runtime.filesystem.record.RecordFormat;
import de.tuberlin.pserver.runtime.state.controller.MatrixDeltaRemoteUpdateController;
import de.tuberlin.pserver.runtime.state.controller.MatrixRemoteUpdateController;
import de.tuberlin.pserver.runtime.state.controller.RemoteUpdateController;
import de.tuberlin.pserver.runtime.state.controller.VectorRemoteUpdateController;
import de.tuberlin.pserver.runtime.state.filter.MatrixUpdateFilter;
import de.tuberlin.pserver.runtime.state.merger.MatrixUpdateMerger;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.PartitionType;
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

    private final List<StateDeclaration> stateDecls;

    private final DataManager dataManager;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MLProgramLinker(final MLProgramContext programContext,
                           final Class<? extends MLProgram> programClass) {

        this.programContext   = Preconditions.checkNotNull(programContext);
        this.programClass     = Preconditions.checkNotNull(programClass);
        this.dataManager      = programContext.runtimeContext.dataManager;
        this.stateDecls       = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void link(final SlotContext slotContext, final MLProgram instance) throws Exception {

        slotContext.CF.select().slot(0).exe(() -> {

            final String slotIDStr = "[" + slotContext.programContext.runtimeContext.nodeID
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

            Thread.sleep(5000); // TODO: Wait until all objects are placed in DHT and accessible...

            slotContext.programContext.programLoadBarrier.countDown();

            final long end = System.currentTimeMillis();

            LOG.info(slotIDStr + "Leave " + slotContext.programContext.simpleClassName
                    + " loading linking [duration: " + (end - start) + " ms].");
        });
    }

    public void fetchStateObjects(final MLProgram program) throws Exception {

        for (final StateDeclaration decl : stateDecls) {

            final Field f = programClass.getDeclaredField(decl.name);

            f.set(program, dataManager.getObject(decl.name));
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
                            stateProperties.partitionType(),
                            stateProperties.rows(),
                            stateProperties.cols(),
                            stateProperties.layout(),
                            stateProperties.format(),
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
                if (an instanceof DeltaFilter) {
                    final DeltaFilter filterProperties = (DeltaFilter) an;
                    StringTokenizer st = new StringTokenizer(filterProperties.stateObjects(), ",");
                    while (st.hasMoreTokens()) {
                        final String stateObjName = st.nextToken().replaceAll("\\s+","");
                        final RemoteUpdateController remoteUpdateController =
                                (RemoteUpdateController)programContext.get(remoteUpdateControllerName(stateObjName));
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
                if (an instanceof DeltaMerger) {
                    final DeltaMerger mergerProperties = (DeltaMerger) an;
                    StringTokenizer st = new StringTokenizer(mergerProperties.stateObjects(), ",");
                    while (st.hasMoreTokens()) {
                        final String stateObjName = st.nextToken().replaceAll("\\s+", "");
                        final RemoteUpdateController remoteUpdateController =
                                (RemoteUpdateController)programContext.get(remoteUpdateControllerName(stateObjName));
                        if (remoteUpdateController == null)
                            throw new IllegalStateException();
                        final MatrixUpdateMerger merger = (MatrixUpdateMerger) field.get(instance);
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
                                                new MatrixRemoteUpdateController(slotContext, decl.name, (Matrix)so));
                                    break;
                                case DELTA_MERGE_UPDATE:
                                        programContext.put(remoteUpdateControllerName(decl.name),
                                                new MatrixDeltaRemoteUpdateController(slotContext, decl.name, (Matrix)so));
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
                                    decl.format == Format.SPARSE_FORMAT
                                            ? RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD)
                                            : RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROW_RECORD),
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
                                    decl.format == Format.SPARSE_FORMAT
                                            ? RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROWCOLVAL_RECORD)
                                            : RecordFormat.DEFAULT.setRecordFactory(IRecordFactory.ROW_RECORD),
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
                                        new VectorRemoteUpdateController(slotContext, decl.name, (Vector)so));
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

    // ---------------------------------------------------
    // Utility Methods.
    // ---------------------------------------------------

    public static String stateDeclarationName(final String name) { return "__state_declaration_" + name; }

    public static String remoteUpdateControllerName(final String name) { return "__remote_update_controller_" + name; }

}
