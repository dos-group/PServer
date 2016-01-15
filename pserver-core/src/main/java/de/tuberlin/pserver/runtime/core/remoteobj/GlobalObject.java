package de.tuberlin.pserver.runtime.core.remoteobj;

import de.tuberlin.pserver.runtime.core.events.EventDispatcher;
import de.tuberlin.pserver.runtime.core.network.NetManager;

import java.lang.reflect.*;
import java.util.*;

public class GlobalObject<T> extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Map<Integer, Method> globalMethods;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public GlobalObject(NetManager netManager, T instance) {
        super(true);

        this.globalMethods = new HashMap<>();
        List<Method> methods = Arrays.asList(Object.class.getMethods());
        //methods.addAll(Arrays.asList(EventDispatcher.class.getMethods()));
        for (Method method : instance.getClass().getMethods()) {
            if (!methods.contains(method) && !Modifier.isStatic(method.getModifiers())) {
                globalMethods.put(MethodInvokeMsg.getMethodID(method), method);
            }
        }

        netManager.addEventListener(MethodInvokeMsg.METHOD_INVOCATION_EVENT, (event) -> {
            MethodInvokeMsg mim = (MethodInvokeMsg) event;
            Method calledMethod = globalMethods.get(mim.methodID);
            Object res = null;

            if (calledMethod == null)
                throw new UnsupportedOperationException("msg.methodID = " + mim.methodID);

            try {
                res = calledMethod.invoke(instance, mim.arguments);
            } catch (IllegalAccessException | InvocationTargetException e) {
                res = e;
            }

            mim.netChannel.sendMsg(
                    new MethodInvokeMsg(
                            mim.callID,
                            mim.classID,
                            mim.methodID,
                            null,
                            res
                    )
            );
        });
    }
}