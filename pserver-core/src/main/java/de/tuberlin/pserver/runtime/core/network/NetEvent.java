package de.tuberlin.pserver.runtime.core.network;

import de.tuberlin.pserver.runtime.core.events.Event;
import de.tuberlin.pserver.commons.json.GsonUtils;

import java.io.Serializable;
import java.util.UUID;


public class NetEvent extends Event {

    public static final class NetEventTypes {

        private NetEventTypes() {}

        public static final String IO_EVENT_CHANNEL_CONNECTED = "con";

        public static final String IO_EVENT_RPC_CALLER_REQUEST = "req";

        public static final String IO_EVENT_RPC_CALLER_RESPONSE = "res";

        public static final String ECHO_REQUEST = "ECHO_REQUEST";

        public static final String ECHO_RESPONSE = "ECHO_RESPONSE";
    }

    public transient NetChannel netChannel;

    //@GsonUtils.Exclude
    public UUID srcMachineID;

    //@GsonUtils.Exclude
    public UUID dstMachineID;

    public NetEvent(final String type, final boolean isSticky) { super(type, isSticky); }
    public NetEvent(String type, Object payload) { super(type, payload); }
    public NetEvent(String type) { super(type, null); }

    public String toString() {
        return "event: " + type + " src: " + srcMachineID + " dst: " + dstMachineID;
    }
}