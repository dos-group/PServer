package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.transaction.TransactionController;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.runtime.RuntimeContext;
import org.apache.commons.lang3.ArrayUtils;

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
        analyzeState();
        analyzeTransactions(instance);
        return programTable;
    }

    // ---------------------------------------------------
    // Private Methods.
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
        for (final Field field : programClass.getDeclaredFields()) {
            for (final Annotation an : field.getDeclaredAnnotations()) {
                if (an instanceof State) {
                    final State stateProperties = (State) an;
                    final StateDescriptor state = StateDescriptor.fromAnnotatedField(
                            stateProperties,
                            field,
                            IntStream.iterate(0, x -> x + 1).limit(runtimeContext.numOfNodes).toArray()
                    );
                    programTable.addState(state);
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
                            programTable.getState(transactionProperties.state()).atNodes
                    );
                    final TransactionController controller = new TransactionController(runtimeContext, descriptor);
                    programTable.addTransactionController(controller);
                }
            }
        }
    }
}
