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

        public final int nodeID;

        public final int threadID;

        public final Class<?> exeClass;

        public final Object stateObj;

        public ExecutionDescriptor(final int nodeID,
                                   final int threadID,
                                   final Class<?> exeClass,
                                   final Object stateObj) {

            this.nodeID     = nodeID;
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
        final InstanceContext ctx = dataManager.getInstanceContext();
        final ExecutionDescriptor entry = new ExecutionDescriptor(
                ctx.jobContext.nodeID,
                ctx.threadID,
                Preconditions.checkNotNull(exeClass),
                Preconditions.checkNotNull(stateObj));

        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobContext.jobUID);
        Preconditions.checkState(descriptors[ctx.threadID] == null);
        descriptors[ctx.threadID] = entry;
    }

    // Must be called in the thread where the algorithm is executed.
    public void unregisterAlgorithm() {
        final InstanceContext ctx = dataManager.getInstanceContext();
        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobContext.jobUID);
        Preconditions.checkState(descriptors[ctx.threadID] != null);
        descriptors[ctx.threadID] = null;
    }

    public void deleteExecutionContext(final UUID jobID) {
        Preconditions.checkNotNull(jobID);
        final InstanceContext ctx = dataManager.getInstanceContext();
        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobContext.jobUID);
        for (final ExecutionDescriptor ed : descriptors)
            Preconditions.checkState(ed == null);
        jobExeDesc.remove(jobID);
    }

    public ExecutionDescriptor[] getExecutionDescriptors(final UUID jobID) {
        final InstanceContext ctx = dataManager.getInstanceContext();
        final ExecutionDescriptor[] descriptors = jobExeDesc.get(ctx.jobContext.jobUID);
        Preconditions.checkState(descriptors[ctx.threadID] != null);
        return descriptors;
    }

    public void putJobScope(final String name, final Object obj) {
        final InstanceContext ctx = dataManager.getInstanceContext();
        Map<String, Object> objs = jobScopeObjects.get(ctx.jobContext.jobUID);
        if (objs == null) {
            objs = new NonBlockingHashMap<>();
            jobScopeObjects.put(ctx.jobContext.jobUID, objs);
        }
        objs.put(Preconditions.checkNotNull(name), obj);
    }

    public Object getJobScope(final String name) {
        final InstanceContext ctx = dataManager.getInstanceContext();
        Map<String, Object> objs = jobScopeObjects.get(ctx.jobContext.jobUID);
        return objs.get(name);
    }
}
