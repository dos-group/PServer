package de.tuberlin.pserver.runtime.driver;


import de.tuberlin.pserver.compiler.GlobalObjectDescriptor;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObject;
import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObjectProxy;
import org.apache.commons.lang3.ArrayUtils;

public final class GlobalObjectAllocator {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public Object alloc(final ProgramContext programContext, final GlobalObjectDescriptor globalObject) {

        Object gObject = null;

        try {

            if (globalObject.atNodes.length != 1)
                throw new UnsupportedOperationException();

            if (ArrayUtils.contains(globalObject.atNodes, programContext.nodeID)) {

                gObject = globalObject.stateImpl.newInstance(); // Requires standard constructor...
                new GlobalObject<>(programContext.runtimeContext.netManager, gObject, globalObject.stateName);

            } else {

                MachineDescriptor machineDescriptor = programContext.runtimeContext.infraManager.getMachine(globalObject.atNodes[0]);

                gObject = GlobalObjectProxy.create(
                        globalObject.stateName,
                        programContext.runtimeContext.netManager,
                        machineDescriptor,
                        globalObject.stateType
                );
            }

        } catch(Throwable t) {
            throw new IllegalStateException(t);
        }

        return gObject;
    }
}
