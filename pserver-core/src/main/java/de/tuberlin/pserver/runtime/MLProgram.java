package de.tuberlin.pserver.runtime;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.EventDispatcher;
import de.tuberlin.pserver.dsl.controlflow.ControlFlowFactory;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.State;
import de.tuberlin.pserver.dsl.state.StateDeclaration;
import de.tuberlin.pserver.dsl.dataflow.DataFlowFactory;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.Layout;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.matrix.dense.DMatrix;
import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.VectorBuilder;
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

public abstract class MLProgram extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected static final Logger LOG = LoggerFactory.getLogger(MLProgram.class);

    protected ExecutionManager executionManager;

    protected DataManager dataManager;

    protected ControlFlowFactory CF;

    protected DataFlowFactory DF;

    public SlotContext slotContext;

    private Program program;

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

        this.CF = new ControlFlowFactory(this.slotContext);

        this.DF = new DataFlowFactory(this.slotContext);

        this.program = new Program(slotContext);
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

        final List<StateDeclaration> stateDecls  = extractStateDeclarations(this.getClass());

        program.enter();

            if (slotContext.slotID == 0) {

                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " loading phase.");

                final long start = System.currentTimeMillis();

                createStateObjects(stateDecls);

                slotContext.programContext.runtimeContext.dataManager.loadInputData(slotContext);

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName
                        + " loading phase [duration: " + (end - start) + " ms].");


                Thread.sleep(5000); // TODO: Wait until all objects are placed in DHT and accessible...

                slotContext.programContext.programLoadBarrier.countDown();
            }

            slotContext.programContext.programLoadBarrier.await();

            fetchState(getClass(), this, stateDecls);

            if (slotContext.slotID == 0) {

                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " initialization phase.");

                final long start = System.currentTimeMillis();

                if (program.initPhase != null) program.initPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName
                        + " initialization phase [duration: " + (end - start) + " ms].");

                slotContext.programContext.programInitBarrier.countDown();
            }

            slotContext.programContext.programInitBarrier.await();

            {
                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " pre-process phase.");

                final long start = System.currentTimeMillis();

                if (program.preProcessPhase != null) program.preProcessPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName
                        + " pre-process phase [duration: " + (end - start) + " ms].");
            }

            {
                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " process phase.");

                final long start = System.currentTimeMillis();

                if (program.processPhase != null) program.processPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName +
                        " process phase [duration: " + (end - start) + " ms].");
            }

            {
                LOG.info("Enter " + program.slotContext.programContext.simpleClassName + " post-process phase.");

                final long start = System.currentTimeMillis();

                if (program.postProcessPhase != null) program.postProcessPhase.body();

                final long end = System.currentTimeMillis();

                LOG.info("Leave " + program.slotContext.programContext.simpleClassName
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

    private List<StateDeclaration> extractStateDeclarations(final Class<? extends MLProgram> programClass) {
        Preconditions.checkNotNull(programClass);
        final List<StateDeclaration> decls = new ArrayList<>();
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof State) {
                    final State properties = (State) an;
                    final StateDeclaration decl = new StateDeclaration(
                            field.getName(),
                            field.getType(),
                            properties.localScope(),
                            properties.globalScope(),
                            properties.partitionType(),
                            properties.rows(),
                            properties.cols(),
                            properties.layout(),
                            properties.format(),
                            properties.path()
                    );
                    decls.add(decl);
                }
            }
        }
        return decls;
    }

    private void createStateObjects(final List<StateDeclaration> stateDecls) {
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
                        final SharedObject so = new DistributedMatrix(
                                slotContext,
                                decl.rows, decl.cols,
                                PartitionType.ROW_PARTITIONED,
                                decl.layout,
                                decl.format,
                                false
                        );
                        dataManager.putObject(decl.name, so);
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

                LOG.info("Fetch object '" + decl.name + "'.");
            }
        } catch(Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
