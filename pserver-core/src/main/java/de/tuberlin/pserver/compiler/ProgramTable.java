package de.tuberlin.pserver.compiler;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.transaction.TransactionController;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class ProgramTable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Class<? extends Program> programClass;

    private final Map<String, StateDescriptor> stateDescriptors;

    private final Map<String, GlobalObjectDescriptor> globalObjectDescriptors;

    private final Map<String, UnitDescriptor> unitDescriptors;

    private final Map<String, TransactionDescriptor> transactionDescriptors;

    private final Map<String, TransactionController> transactionControllers;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ProgramTable(Class<? extends Program> programClass) {

        this.programClass = Preconditions.checkNotNull(programClass);

        this.stateDescriptors = new TreeMap<>();

        this.globalObjectDescriptors = new TreeMap<>();

        this.unitDescriptors = new TreeMap<>();

        this.transactionDescriptors = new TreeMap<>();

        this.transactionControllers = new TreeMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Class<? extends Program> getProgramClass() {
        return programClass;
    }

    // ---------------------------------------------------

    public void addState(final StateDescriptor state) {
        stateDescriptors.put(state.stateName, Preconditions.checkNotNull(state));
    }

    public void addGlobalObject(final GlobalObjectDescriptor globalObject) {
        globalObjectDescriptors.put(globalObject.stateName, globalObject);
    }

    public void addTransaction(final TransactionDescriptor transaction) {
        transactionDescriptors.put(transaction.transactionName, Preconditions.checkNotNull(transaction));
    }

    public void addTransactionController(final TransactionController transactionController) {
        transactionControllers.put(transactionController.getTransactionDescriptor().transactionName, Preconditions.checkNotNull(transactionController));
    }

    public void addUnit(final UnitDescriptor unit) {
        unitDescriptors.put(unit.unitName, Preconditions.checkNotNull(unit));
    }

    // ---------------------------------------------------

    public StateDescriptor getState(final String stateName) {
        return stateDescriptors.get(stateName);
    }

    public GlobalObjectDescriptor getGlobalObject(final String globalObjectName) {
        return globalObjectDescriptors.get(globalObjectName);
    }

    public TransactionDescriptor getTransaction(final String transactionName) {
        return transactionDescriptors.get(transactionName);
    }

    public TransactionController getTransactionController(final String transactionName) {
        return transactionControllers.get(transactionName);
    }

    public UnitDescriptor getUnit(final String unitName) {
        return unitDescriptors.get(unitName);
    }

    // ---------------------------------------------------

    public Collection<StateDescriptor> getState() { return stateDescriptors.values(); }

    public Collection<GlobalObjectDescriptor> getGlobalObjects() { return globalObjectDescriptors.values(); }

    public Collection<TransactionDescriptor> getTransactions() { return transactionDescriptors.values(); }

    public Collection<TransactionController> getTransactionControllers() { return transactionControllers.values(); }

    public Collection<UnitDescriptor> getUnits() { return unitDescriptors.values(); }
}
