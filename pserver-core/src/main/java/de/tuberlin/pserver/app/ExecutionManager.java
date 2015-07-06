package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.utils.nbhm.NonBlockingHashMap;

import java.util.Map;
import java.util.UUID;

public class ExecutionManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class ExecutionDescriptor {

        public final int instanceID;

        public final int threadID;

        //public final PServerJob instance;

        public final Class<?> exeClass;

        public final Object stateObj;

        public ExecutionDescriptor(final int instanceID,
                                   final int threadID,
                                   final Class<?> exeClass,
                                   final Object stateObj) {

            this.instanceID = instanceID;
            this.threadID   = threadID;
            this.exeClass   = Preconditions.checkNotNull(exeClass);
            this.stateObj   = stateObj;
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final DataManager dataManager;

    private final Map<UUID, ExecutionDescriptor[]> jobExeDesc;

    private final Map<UUID, Map<String, Object>> jobScopeObjects;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ExecutionManager(final DataManager dataManager) {
        this.dataManager     = Preconditions.checkNotNull(dataManager);
        this.jobExeDesc      = new NonBlockingHashMap<>();
        this.jobScopeObjects = new NonBlockingHashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void createExecutionContext(final UUID jobID, final int perNodeParallelism) {
        Preconditions.checkNotNull(jobID);
        final ExecutionDescriptor[] descriptors = new ExecutionDescriptor[perNodeParallelism];
        jobExeDesc.put(jobID, descriptors);
    }

    // Must be called in the thread where the algorithm is executed.
    public void registerAlgorithm(final Class<?> exeClass, final Object stateObj) {
        final PServerContext ctx = dataManager.getJobContext();
        final ExecutionDescriptor entry = new ExecutionDescriptor(
                ctx.instanceID,
                ctx.threadID,
                Preconditions.checkNotNull(exeClass),
                Preconditions.checkNotNull(stateObj));

        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobUID);
        Preconditions.checkState(descriptors[ctx.threadID] == null);
        descriptors[ctx.threadID] = entry;
    }

    // Must be called in the thread where the algorithm is executed.
    public void unregisterAlgorithm() {
        final PServerContext ctx = dataManager.getJobContext();
        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobUID);
        Preconditions.checkState(descriptors[ctx.threadID] != null);
        descriptors[ctx.threadID] = null;
    }

    public void deleteExecutionContext(final UUID jobID) {
        Preconditions.checkNotNull(jobID);
        final PServerContext ctx = dataManager.getJobContext();
        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobUID);
        for (final ExecutionDescriptor ed : descriptors)
            Preconditions.checkState(ed == null);
        jobExeDesc.remove(jobID);
    }

    public ExecutionDescriptor[] getExecutionDescriptors(final UUID jobID) {
        final PServerContext ctx = dataManager.getJobContext();
        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobUID);
        Preconditions.checkState(descriptors[ctx.threadID] != null);
        return descriptors;
    }

    public void putJobScope(final String name, final Object obj) {
        final PServerContext ctx = dataManager.getJobContext();
        Map<String, Object> objs = jobScopeObjects.get(ctx.jobUID);
        if (objs == null) {
            objs = new NonBlockingHashMap<>();
            jobScopeObjects.put(ctx.jobUID, objs);
        }
        objs.put(Preconditions.checkNotNull(name), obj);
    }

    public Object getJobScope(final String name) {
        final PServerContext ctx = dataManager.getJobContext();
        Map<String, Object> objs = jobScopeObjects.get(ctx.jobUID);
        return objs.get(name);
    }
}
