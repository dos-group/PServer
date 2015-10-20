package de.tuberlin.pserver.runtime.state.types;


import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.runtime.core.net.NetEvents;
import de.tuberlin.pserver.runtime.core.net.NetManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

public class RemoteMatrixStub {

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public RemoteMatrixStub(final ProgramContext programContext,
                            final String name,
                            final Matrix matrix) {

        final NetManager netManager = programContext.runtimeContext.netManager;

        netManager.addEventListener("get_request_" + name, event -> {
            final NetEvents.NetEvent getRequestEvent = (NetEvents.NetEvent) event;
            @SuppressWarnings("unchecked")
            final Pair<Long, Long> pos = (Pair<Long, Long>) getRequestEvent.getPayload();
            final NetEvents.NetEvent getResponseEvent = new NetEvents.NetEvent("get_response_" + name);
            getResponseEvent.setPayload(matrix.get(pos.getLeft(), pos.getRight()));
            netManager.sendEvent(((NetEvents.NetEvent) event).srcMachineID, getResponseEvent);
        });

        netManager.addEventListener("put_request_" + name, event -> {
            final NetEvents.NetEvent putRequestEvent = (NetEvents.NetEvent) event;
            @SuppressWarnings("unchecked")
            final Triple<Long, Long, Double> pos = (Triple<Long, Long, Double>) putRequestEvent.getPayload();
            matrix.set(pos.getLeft(), pos.getMiddle(), pos.getRight());
        });

    }
}
