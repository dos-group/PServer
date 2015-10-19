package de.tuberlin.pserver.runtime.events;


import de.tuberlin.pserver.commons.ds.ResettableCountDownLatch;
import de.tuberlin.pserver.runtime.core.events.Event;
import de.tuberlin.pserver.runtime.core.events.IEventDispatcher;
import de.tuberlin.pserver.runtime.core.events.IEventHandler;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.net.NetEvents;

public abstract class MsgEventHandler implements IEventHandler {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String MSG_EVENT_PREFIX = "msg_event_prefix_";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private ResettableCountDownLatch latch = null;

    private InfrastructureManager infraManager;

    private IEventDispatcher dispatcher;

    private boolean removeAfterAwait;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public abstract void handleMsg(final int srcNodeID, final Object value);

    @Override
    public void handleEvent(final Event e) {
        final NetEvents.NetEvent event = (NetEvents.NetEvent) e;
        final int srcNodeID = infraManager.getNodeIDFromMachineUID(event.srcMachineID);
        handleMsg(srcNodeID, event.getPayload());
        latch.countDown();

        if (removeAfterAwait && latch.getCount() == 0)
            dispatcher.removeEventListener(event.type, this);
    }

    public void initLatch(final int n) { latch = new ResettableCountDownLatch(n); }

    public void reset() { latch.reset(); }

    public ResettableCountDownLatch getLatch() { return latch; }

    public void setDispatcher(final IEventDispatcher dispatcher) { this.dispatcher = dispatcher; }

    public void setInfraManager(final InfrastructureManager infraManager) { this.infraManager = infraManager; }

    public void setRemoveAfterAwait(final boolean removeAfterAwait) { this.removeAfterAwait = removeAfterAwait; }
}
