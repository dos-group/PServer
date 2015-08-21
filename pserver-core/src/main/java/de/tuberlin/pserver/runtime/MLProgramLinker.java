package de.tuberlin.pserver.runtime;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.state.*;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
import de.tuberlin.pserver.runtime.delta.MatrixDelta;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaFilter;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaManager;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaMerger;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.runtime.filesystem.record.RecordFormat;
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

                programContext.put(getStateDeclKey(decl.name), decl);
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
                            stateProperties.delta()
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
                        final MatrixDeltaManager deltaManager =
                                (MatrixDeltaManager)programContext.get(getDeltaManagerKey(stateObjName));
                        if (deltaManager == null)
                            throw new IllegalStateException();
                        final MatrixDeltaFilter filter = (MatrixDeltaFilter)field.get(instance);
                        deltaManager.setDeltaFilter(filter);
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
                        final MatrixDeltaManager deltaManager =
                                (MatrixDeltaManager)programContext.get(getDeltaManagerKey(stateObjName));
                        if (deltaManager == null)
                            throw new IllegalStateException();
                        final MatrixDeltaMerger merger = (MatrixDeltaMerger) field.get(instance);
                        deltaManager.setDeltaMerger(merger);
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

                            if (decl.delta == DeltaUpdate.LZ4_DELTA) {
                                if (decl.format == Format.DENSE_FORMAT) {
                                    final MatrixDelta delta = new MatrixDelta(slotContext, (DMatrix)so);
                                    final EmbeddedDHTObject<MatrixDelta> dhtObjectDelta = new EmbeddedDHTObject<>(delta);
                                    final MatrixDeltaManager deltaManager =
                                            new MatrixDeltaManager(dhtObjectDelta);
                                    programContext.put(getDeltaManagerKey(decl.name), deltaManager);
                                    dataManager.putObject(getDeltaObjectKey(decl.name), dhtObjectDelta);
                                } else
                                    throw new UnsupportedOperationException();
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

    public static String getStateDeclKey(final String name) { return STATE_DECLARATION_PREFIX + name; }

    public static String getDeltaManagerKey(final String name) { return DELTA_MANAGER_PREFIX + name; }

    public static String getDeltaObjectKey(final String name) { return DELTA_OBJECT_PREFIX + name; }

    // ---------------------------------------------------
    // Private Constants.
    // ---------------------------------------------------

    private static final String DELTA_MANAGER_PREFIX     = "__delta_manager_";

    private static final String DELTA_OBJECT_PREFIX      = "__delta_object_";

    private static final String STATE_DECLARATION_PREFIX = "__state_decl_";
}
