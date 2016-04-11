package de.tuberlin.pserver.runtime.driver;


import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObject;
import de.tuberlin.pserver.runtime.state.matrix.rpc.GlobalStateMatrixProxy;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

public final class StateAllocator {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public Pair<Matrix32F, Matrix32F> alloc(ProgramContext programContext, DistributedTypeInfo state)
            throws Exception {

        Matrix32F stateObj = null, proxyObj = null;

            if (ArrayUtils.contains(state.nodes(), state.nodeId())) {

                stateObj = (Matrix32F) state;

                new GlobalObject<>(programContext.runtimeContext.netManager, stateObj, state.name());

            } else {

                if (state.hasGlobalAccess())
                    proxyObj = GlobalStateMatrixProxy.create(programContext, state);
            }

        return Pair.of(stateObj, proxyObj);
    }
}
