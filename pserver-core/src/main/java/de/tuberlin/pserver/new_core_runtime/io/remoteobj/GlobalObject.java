package de.tuberlin.pserver.new_core_runtime.io.remoteobj;


import de.tuberlin.pserver.new_core_runtime.events.EventDispatcher;
import de.tuberlin.pserver.new_core_runtime.io.network.NetChannel;
import de.tuberlin.pserver.new_core_runtime.io.network.NetManager;

import java.lang.reflect.*;
import java.util.*;

public abstract class GlobalObject extends EventDispatcher {

    private final Map<Integer, Method> globalMethods;

    public GlobalObject(NetManager netManager) {
        super(true);

        this.globalMethods = new HashMap<>();
        List<Method> methods = Arrays.asList(Object.class.getMethods());
        //methods.addAll(Arrays.asList(EventDispatcher.class.getMethods()));
        for (Method method : getClass().getMethods()) {
            if (!methods.contains(method) && !Modifier.isStatic(method.getModifiers())) {
                globalMethods.put(MethodInvokeMsg.getMethodID(method), method);
            }
        }

        netManager.addMsgHandler(MethodInvokeMsg.class, (NetChannel channel, MethodInvokeMsg msg) -> {
            Method calledMethod = globalMethods.get(msg.methodID);
            Object res = null;

            if (calledMethod == null)
                throw new UnsupportedOperationException("msg.methodID = " + msg.methodID);

            try {
                res = calledMethod.invoke(GlobalObject.this, msg.arguments);
            } catch (IllegalAccessException | InvocationTargetException e) {
                res = e;
            }

            channel.sendMsg(
                new MethodInvokeMsg(
                    msg.callID,
                    msg.classID,
                    msg.methodID,
                    null,
                    res
                )
            );
        });
    }
}