package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.state.annotations.GlobalObject;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.runtime.RuntimeContext;
import de.tuberlin.pserver.types.PServerTypeFactory;
import de.tuberlin.pserver.types.metadata.DistributedDeclaration;
import de.tuberlin.pserver.types.metadata.DistributedType;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.stream.IntStream;

public final class Compiler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Class<? extends Program> programClass;

    private final RuntimeContext runtimeContext;

    private final ProgramTable programTable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Compiler(final RuntimeContext runtimeContext, final Class<? extends Program> programClass) {
        this.programClass     = Preconditions.checkNotNull(programClass);
        this.runtimeContext   = runtimeContext;
        this.programTable     = new ProgramTable(programClass);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public ProgramTable compile(final Program instance, final int nodeDOP) throws Exception {
        analyzeUnits(nodeDOP);
        analyzeGlobalObjects();
        analyzeState();
        analyzeTransactions(instance);
        return programTable;
    }

    // ---------------------------------------------------
    // Analyze Annotations.
    // ---------------------------------------------------

    private void analyzeUnits(int nodeDOP) {
        final int[] nodeIDs = IntStream.iterate(0, x -> x + 1).limit(nodeDOP).toArray();
        UnitDescriptor globalUnit = null;
        for (final Method method : programClass.getDeclaredMethods()) {
            for (final Annotation an : method.getDeclaredAnnotations()) {
                if (an instanceof Unit) {
                    final UnitDescriptor unit = UnitDescriptor.fromAnnotatedMethod(method, (Unit) an);
                    if (unit.atNodes.length > 0 ) {
                        ArrayUtils.removeElements(nodeIDs, unit.atNodes);
                    } else {
                        if (globalUnit != null)
                            throw new IllegalStateException();
                        globalUnit = unit;
                    }
                    programTable.addUnit(unit);
                }
            }
        }

        if (globalUnit != null) {
            globalUnit.atNodes = nodeIDs;
        }
    }

    private void analyzeState() {
        int[] allNodes = IntStream.iterate(0, x -> x + 1).limit(runtimeContext.numOfNodes).toArray();
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation typeAn : field.getDeclaredAnnotations()) {

                if (PServerTypeFactory.isSupportedType(typeAn.annotationType())) {

                    Pair<DistributedDeclaration, DistributedType> declAndInstance =
                            PServerTypeFactory.newInstance(runtimeContext.nodeID, allNodes, field.getType(), field.getName(), typeAn);

                    StateDescriptor state = new StateDescriptor(
                            declAndInstance.getLeft(),
                            declAndInstance.getRight()
                    );

                    programTable.addState(state);
                }
            }
        }
    }

    private void analyzeGlobalObjects() {
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof GlobalObject) {
                    final GlobalObject globalObjectProperties = (GlobalObject) an;
                    final GlobalObjectDescriptor globalObject = GlobalObjectDescriptor.fromAnnotatedField(
                            globalObjectProperties,
                            field,
                            IntStream.iterate(0, x -> x + 1).limit(runtimeContext.numOfNodes).toArray()
                    );
                    programTable.addGlobalObject(globalObject);
                }
            }
        }
    }

    private void analyzeTransactions(final Program instance) throws Exception {
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof Transaction) {
                    final Transaction transactionProperties = (Transaction) an;
                    final TransactionDescriptor descriptor = TransactionDescriptor.fromAnnotatedField(
                            instance,
                            transactionProperties,
                            field,
                            runtimeContext.nodeID,
                            programTable
                    );
                    final TransactionController controller = new TransactionController(runtimeContext, descriptor);
                    programTable.addTransactionController(controller);
                }
            }
        }
    }
}
