package de.tuberlin.pserver.runtime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.controlflow.ControlFlow;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.*;
import de.tuberlin.pserver.dsl.dataflow.DataFlow;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaManager;
import de.tuberlin.pserver.runtime.delta.MatrixDelta;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaFilter;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaMerger;
import de.tuberlin.pserver.runtime.dht.types.EmbeddedDHTObject;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordFactory;
import de.tuberlin.pserver.runtime.filesystem.record.RecordFormat;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.PartitionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public abstract class MLProgram extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(MLProgram.class);

    protected ExecutionManager executionManager;

    protected DataManager dataManager;

    public SlotContext slotContext;

    private Program program;

    // ---------------------------------------------------

    public ControlFlow CF;

    public DataFlow DF;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MLProgram() {
        super(true);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void injectContext(final SlotContext slotContext) {

        this.slotContext = Preconditions.checkNotNull(slotContext);

        this.executionManager = slotContext.programContext.runtimeContext.executionManager;

        this.dataManager = slotContext.programContext.runtimeContext.dataManager;

        this.program = new Program(slotContext);

        this.CF = slotContext.CF;

        this.DF = slotContext.DF;
    }

    public void result(final Serializable... obj) {
        if (slotContext.slotID == 0) {
            dataManager.setResults(slotContext.programContext.programID, Arrays.asList(obj));
        }
    }

    // ---------------------------------------------------
    // Lifecycle.
    // ---------------------------------------------------

    public void define(final Program program) {}

    // ---------------------------------------------------
    // Lifecycle Execution.
    // ---------------------------------------------------

    public void run() throws Exception {

        define(program);

        final List<StateDeclaration> stateDecls  = extractStateAnnotations(this.getClass());

        final String slotIDStr = "[" + slotContext.programContext.runtimeContext.nodeID
                + " | " + slotContext.slotID + "] ";

        program.enter();

            if (slotContext.slotID == 0) {

                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " loading phase.");

                final long start = System.currentTimeMillis();

                createStateObjects(stateDecls);

                extractAndApplyDeltaFilterAnnotations(this.getClass());

                extractAndApplyDeltaMergerAnnotations(this.getClass());

                slotContext.programContext.runtimeContext.dataManager.loadInputData(slotContext);

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName
                        + " loading phase [duration: " + (end - start) + " ms].");

                Thread.sleep(5000); // TODO: Wait until all objects are placed in DHT and accessible...

                slotContext.programContext.programLoadBarrier.countDown();
            }

            slotContext.programContext.programLoadBarrier.await();

            fetchState(getClass(), this, stateDecls);

            if (slotContext.slotID == 0) {

                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " initialization phase.");

                final long start = System.currentTimeMillis();

                if (program.initPhase != null) program.initPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName
                        + " initialization phase [duration: " + (end - start) + " ms].");

                slotContext.programContext.programInitBarrier.countDown();
            }

            slotContext.programContext.programInitBarrier.await();

            {
                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " pre-process phase.");

                final long start = System.currentTimeMillis();

                if (program.preProcessPhase != null) program.preProcessPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName
                        + " pre-process phase [duration: " + (end - start) + " ms].");
            }

            {
                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " process phase.");

                final long start = System.currentTimeMillis();

                if (program.processPhase != null) program.processPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName +
                        " process phase [duration: " + (end - start) + " ms].");
            }

            {
                LOG.info(slotIDStr + "Enter " + program.slotContext.programContext.simpleClassName + " post-process phase.");

                final long start = System.currentTimeMillis();

                if (program.postProcessPhase != null) program.postProcessPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info(slotIDStr + "Leave " + program.slotContext.programContext.simpleClassName
                        + " post-process phase [duration: " + (end - start) + " ms].");
            }

            slotContext.programContext.programDoneBarrier.countDown();

            if (slotContext.slotID == 0)
                slotContext.programContext.programDoneBarrier.await();

        program.leave();
    }

    // ---------------------------------------------------
    // State-Management.
    // ---------------------------------------------------

    private List<StateDeclaration> extractStateAnnotations(final Class<? extends MLProgram> programClass) {
        Preconditions.checkNotNull(programClass);
        final List<StateDeclaration> decls = new ArrayList<>();
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
                    decls.add(decl);
                }
            }
        }
        return decls;
    }

    private void extractAndApplyDeltaFilterAnnotations(final Class<? extends MLProgram> programClass) throws Exception {
        for (final Field field : Preconditions.checkNotNull(programClass).getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof DeltaFilter) {
                    final DeltaFilter filterProperties = (DeltaFilter) an;
                    StringTokenizer st = new StringTokenizer(filterProperties.stateObjects(), ",");
                    while (st.hasMoreTokens()) {
                        final String stateObjName = st.nextToken().replaceAll("\\s+","");
                        final MatrixDeltaManager deltaManager =
                                (MatrixDeltaManager)slotContext.programContext.get(stateObjName + "-Delta-Manager");
                        if (deltaManager == null)
                            throw new IllegalStateException();
                        final MatrixDeltaFilter filter = (MatrixDeltaFilter)field.get(this);
                        deltaManager.setDeltaFilter(filter);
                    }
                }
            }
        }
    }

    private void extractAndApplyDeltaMergerAnnotations(final Class<? extends MLProgram> programClass) throws Exception {
        for (final Field field : Preconditions.checkNotNull(programClass).getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof DeltaMerger) {
                    final DeltaMerger mergerProperties = (DeltaMerger) an;
                    StringTokenizer st = new StringTokenizer(mergerProperties.stateObjects(), ",");
                    while (st.hasMoreTokens()) {
                        final String stateObjName = st.nextToken().replaceAll("\\s+", "");
                        final MatrixDeltaManager deltaManager =
                                (MatrixDeltaManager)slotContext.programContext.get(stateObjName + "-Delta-Manager");
                        if (deltaManager == null)
                            throw new IllegalStateException();
                        final MatrixDeltaMerger merger = (MatrixDeltaMerger) field.get(this);
                        deltaManager.setDeltaMerger(merger);
                    }
                }
            }
        }
    }

    private void createStateObjects(final List<StateDeclaration> stateDecls) throws Exception {
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
                                    slotContext.programContext.put(decl.name + "-Delta-Manager", deltaManager);
                                    dataManager.putObject(decl.name + "-Delta", dhtObjectDelta);
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

    private void fetchState(final Class<? extends MLProgram> programClass,
                            final MLProgram program,
                            final List<StateDeclaration> decls) {
        try {
            for (final StateDeclaration decl : decls) {
                final Field f = programClass.getDeclaredField(decl.name);
                f.set(program, dataManager.getObject(decl.name));
                //LOG.info("Fetch object '" + decl.name + "'.");
            }
        } catch(Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
