package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import de.tuberlin.pserver.dsl.state.StateDeclaration;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.TransactionDescriptor;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.unit.UnitDeclaration;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ProgramContext;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.types.DistributedMatrix;
import de.tuberlin.pserver.types.PartitionType;
import de.tuberlin.pserver.types.RemoteMatrixSkeleton;
import de.tuberlin.pserver.types.RemoteMatrixStub;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public final class Compiler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(Compiler.class);

    private final ProgramContext programContext;

    private final Class<? extends Program> programClass;

    private final DataManager dataManager;

    private final RuntimeContext runtimeContext;

    private List<StateDeclaration> stateDecls;

    //private List<TransactionDescriptor> transactionDecls;

    private List<UnitDeclaration> unitDecls;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Compiler(final ProgramContext programContext,
                    final Class<? extends Program> programClass) {

        this.programContext   = Preconditions.checkNotNull(programContext);
        this.programClass     = Preconditions.checkNotNull(programClass);
        this.dataManager      = programContext.runtimeContext.dataManager;
        this.runtimeContext   = programContext.runtimeContext;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void link(final Program instance) throws Exception {

        unitDecls = new ArrayList<>();
        analyzeExecutableUnits();
        stateDecls = new ArrayList<>();
        final String slotIDStr = "[" + runtimeContext.nodeID + "]";
        LOG.info(slotIDStr + "Enter " + programContext.simpleClassName + " linking phase.");
        final long start = System.currentTimeMillis();

        analyzeStateAnnotations();

        for (final StateDeclaration decl : stateDecls) {
            programContext.put(stateDeclarationName(decl.name), decl);
        }

        analyzeTransactionAnnotations(instance);
        allocateStateObjects(programContext);
        dataManager.loadInputData();

        programContext.put(stateDeclarationListName(), stateDecls);

        Thread.sleep(5000); // TODO: Wait until all objects are placed in DHT and accessible...
        final long end = System.currentTimeMillis();
        LOG.info(slotIDStr + "Leave " + programContext.simpleClassName
                + " loading linking [duration: " + (end - start) + " ms].");
    }

    public void fetchStateObjects(final Program program) throws Exception {
        stateDecls = programContext.get(stateDeclarationListName());
        for (final StateDeclaration decl : stateDecls) {
            final Field f = programClass.getDeclaredField(decl.name);
            final Object stateObj = dataManager.getObject(decl.name);
            Preconditions.checkState(stateObj != null);
            f.set(program, stateObj);
        }
    }

    public void defineUnits(final Program programInvokeable, final Lifecycle lifecycle) {
        Preconditions.checkNotNull(programInvokeable);
        Preconditions.checkNotNull(lifecycle);
        Preconditions.checkNotNull(unitDecls);
        for (final UnitDeclaration decl : unitDecls) {
            if (ArrayUtils.contains(decl.atNodes, lifecycle.programContext.runtimeContext.nodeID)) {
                try {
                    decl.method.invoke(programInvokeable, lifecycle);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void analyzeExecutableUnits() {
        final List<Integer> availableNodeIDs  = new ArrayList<>();
        for (int i = 0; i < programContext.nodeDOP; ++i)
            availableNodeIDs.add(i);
        // The global unit has no specific node assignments.
        int globalUnitDeclIndex = -1;
        for (final Method method : programClass.getDeclaredMethods()) {
            for (final Annotation an : method.getDeclaredAnnotations()) {
                if (an instanceof Unit) {
                    if (method.getReturnType() != void.class)
                        throw new IllegalStateException();
                    if (method.getParameterTypes().length != 1)
                        throw new IllegalStateException();
                    if (method.getParameterTypes()[0] != Lifecycle.class)
                        throw new IllegalStateException();

                    final Unit unitProperties = (Unit) an;
                    final int[] executingNodeIDs = parseNodeRanges(unitProperties.at());

                    if (executingNodeIDs.length == 0 && globalUnitDeclIndex == -1) // TODO
                        globalUnitDeclIndex = unitDecls.size();
                    else
                        if (globalUnitDeclIndex != -1)
                            throw new IllegalStateException("globalUnitDeclIndex = " + globalUnitDeclIndex + " | executingNodeIDs.length = " + executingNodeIDs.length);

                    for (final Integer nodeID : executingNodeIDs) {
                        if (!availableNodeIDs.remove(nodeID))
                            throw new IllegalStateException();
                    }

                    unitDecls.add(new UnitDeclaration(method, executingNodeIDs));
                }
            }
        }

        if (globalUnitDeclIndex != -1) {
            if (availableNodeIDs.size() == 0)
                throw new IllegalStateException();
            final UnitDeclaration globalUnitDecl = unitDecls.remove(globalUnitDeclIndex);
            unitDecls.add(new UnitDeclaration(globalUnitDecl.method, Ints.toArray(availableNodeIDs)));
        }
    }

    private void analyzeStateAnnotations() {
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof State) {
                    final State stateProperties = (State) an;
                    final StateDeclaration decl = new StateDeclaration(
                            field.getName(),
                            field.getType(),
                            stateProperties.globalScope(),
                            "".equals(stateProperties.at()) ? dataManager.nodeIDs : parseNodeRanges(stateProperties.at()),
                            stateProperties.partitionType(),
                            stateProperties.rows(),
                            stateProperties.cols(),
                            stateProperties.layout(),
                            stateProperties.format(),
                            stateProperties.recordFormat(),
                            stateProperties.path()
                    );
                    stateDecls.add(decl);
                }
            }
        }
    }

    private void analyzeTransactionAnnotations(final Program instance) throws Exception {
        //transactionDecls = new ArrayList<>();
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof Transaction) {
                    final Transaction transactionProperties = (Transaction) an;
                    final StateDeclaration stateDecl = programContext.get(stateDeclarationName(transactionProperties.state()));
                    final TransactionDescriptor descritpor = new TransactionDescriptor(
                            field.getName(),
                            transactionProperties.state(),
                            (TransactionDefinition)field.get(instance),
                            transactionProperties.type(),
                            transactionProperties.cache(),
                            runtimeContext.nodeID,
                            stateDecl.atNodes
                    );
                    final TransactionController controller = new TransactionController(runtimeContext, descritpor);
                    programContext.put(descritpor.transactionName, controller);
                    //transactionDecls.add(descritpor);
                }
            }
        }
    }

    private void allocateStateObjects(final ProgramContext programContext) throws Exception {
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

                            new RemoteMatrixStub(programContext, decl.name, (Matrix)so);

                        } else {

                            final RemoteMatrixSkeleton remoteMatrixSkeleton = new RemoteMatrixSkeleton(
                                    programContext,
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

                            if (ArrayUtils.contains(decl.atNodes, programContext.runtimeContext.nodeID)) {

                                final SharedObject so = new MatrixBuilder()
                                        .dimension(decl.rows, decl.cols)
                                        .format(decl.format)
                                        .layout(decl.layout)
                                        .build();

                                dataManager.putObject(decl.name, so);
                            }
                        }
                        else {
                            dataManager.loadAsMatrix(
                                    programContext,
                                    decl.path,
                                    decl.name,
                                    decl.rows,
                                    decl.cols,
                                    decl.atNodes,
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

                            if (ArrayUtils.contains(decl.atNodes, programContext.runtimeContext.nodeID)) {

                                final SharedObject so = new DistributedMatrix(
                                        programContext,
                                        decl.rows, decl.cols,
                                        decl.atNodes,
                                        PartitionType.ROW_PARTITIONED,
                                        decl.layout,
                                        decl.format,
                                        false
                                );

                                dataManager.putObject(decl.name, so);
                            }

                        } else {

                            dataManager.loadAsMatrix(
                                    programContext,
                                    decl.path,
                                    decl.name,
                                    decl.rows,
                                    decl.cols,
                                    decl.atNodes,
                                    decl.globalScope,
                                    decl.partitionType,
                                    decl.recordFormatConfigClass.newInstance(),
                                    decl.format,
                                    decl.layout
                            );
                        }
                    } break;
                    case LOGICALLY_PARTITIONED:

                        if (ArrayUtils.contains(decl.atNodes, programContext.runtimeContext.nodeID)) {

                            final SharedObject so = new DistributedMatrix(
                                    programContext,
                                    decl.rows, decl.cols,
                                    decl.atNodes,
                                    PartitionType.ROW_PARTITIONED,
                                    decl.layout,
                                    decl.format,
                                    true
                            );

                            dataManager.putObject(decl.name, so);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } else
                throw new UnsupportedOperationException();
        }
    }

    // ---------------------------------------------------
    // Annotation Property Parsing.
    // ---------------------------------------------------

    private int[] parseNodeRanges(final String rangeDefinition) {

        if (rangeDefinition.contains("-")) { // interval definition

            final StringTokenizer tokenizer = new StringTokenizer(Preconditions.checkNotNull(rangeDefinition), "-");
            final List<Integer> vals = new ArrayList<>();
            final int fromNodeID = Integer.valueOf(tokenizer.nextToken().replaceAll("\\s+", ""));
            final int toNodeID = Integer.valueOf(tokenizer.nextToken().replaceAll("\\s+", ""));

            if (tokenizer.hasMoreTokens())
                throw new IllegalStateException();

            for (int i = fromNodeID; i <= toNodeID; ++i) vals.add(i);

            return Ints.toArray(vals);

        } else { // comma separated definition

            final StringTokenizer tokenizer = new StringTokenizer(Preconditions.checkNotNull(rangeDefinition), ",");
            final List<Integer> vals = new ArrayList<>();

            while (tokenizer.hasMoreTokens())
                vals.add(Integer.valueOf(tokenizer.nextToken().replaceAll("\\s+", "")));

            return Ints.toArray(vals);
        }
    }

    // ---------------------------------------------------
    // Utility Methods.
    // ---------------------------------------------------

    public static String stateDeclarationListName() { return "__state_declarations__"; }

    public static String stateDeclarationName(final String name) { return "__state_declaration_" + name; }
}
