package de.tuberlin.pserver.new_core_runtime.io.remoteobj;

import java.lang.reflect.Method;
import java.util.UUID;


public final class MethodInvokeMsg {

    public final UUID callID;
    public final int classID;
    public final int methodID;
    public final Object[] arguments;
    public final Object result;

    public MethodInvokeMsg() { this(null, -1, -1, null, null); }
    public MethodInvokeMsg(UUID callID, int classID, int methodID, Object[] arguments, Object result) {
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
