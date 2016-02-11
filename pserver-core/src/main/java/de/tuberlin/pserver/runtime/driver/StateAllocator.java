package de.tuberlin.pserver.runtime.driver;


import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.runtime.core.remoteobj.GlobalObject;
import de.tuberlin.pserver.runtime.state.matrix.rpc.GlobalStateMatrixProxy;
import de.tuberlin.pserver.types.matrix.MatrixBuilder;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.properties.ElementType;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.tukaani.xz.UnsupportedOptionsException;

public final class StateAllocator {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @SuppressWarnings("unchecked")
    public Pair<Matrix32F, Matrix32F> alloc(ProgramContext programContext, StateDescriptor state)
            throws Exception {

        Matrix32F stateObj = null, proxyObj = null;

            if (ArrayUtils.contains(state.instance.nodes(), state.instance.nodeId())) {

                stateObj = (Matrix32F) state.instance;

                new GlobalObject<>(programContext.runtimeContext.netManager, stateObj, state.declaration.name);

            } else {

                proxyObj = GlobalStateMatrixProxy.create(programContext, state);
            }

        return Pair.of(stateObj, proxyObj);
    }
}
