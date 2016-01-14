package de.tuberlin.pserver.runtime.core.remoteobj;

import de.tuberlin.pserver.runtime.core.network.NetEvent;

import java.lang.reflect.Method;
import java.util.UUID;


public final class MethodInvokeMsg extends NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String METHOD_INVOCATION_EVENT  = "method_invocation_event";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final UUID callID;

    public final int classID;

    public final int methodID;

    public final Object[] arguments;

    public final Object result;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MethodInvokeMsg() { this(null, -1, -1, null, null); }
    public MethodInvokeMsg(UUID callID, int classID, int methodID, Object[] arguments, Object result) {
        super(METHOD_INVOCATION_EVENT);
        this.callID = callID;
        this.classID = classID;
        this.methodID = methodID;
        this.arguments = arguments;
        this.result = result;
    }

    public static int getMethodID(Method method) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(method.getName());
        for (Class<?> cl : method.getParameterTypes()) {
            strBuilder.append(cl.getSimpleName());
        }
        return strBuilder.toString().hashCode();
    }
}
