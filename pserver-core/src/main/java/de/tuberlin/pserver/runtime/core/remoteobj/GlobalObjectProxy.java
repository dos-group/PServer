package de.tuberlin.pserver.runtime.core.remoteobj;

import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetManager;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public final class GlobalObjectProxy implements InvocationHandler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final MachineDescriptor remoteNetDescriptor;

    private final Class<?> classType;

    private final NetManager netManager;

    private final Map<UUID, CountDownLatch> requestLatches;

    private final Map<UUID, Object> resultObjects;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private GlobalObjectProxy(NetManager netManager, MachineDescriptor remoteNetDescriptor, Class<?> classType) {
        this.netManager = netManager;
        this.remoteNetDescriptor = remoteNetDescriptor;
        this.classType = classType;
        this.requestLatches = new ConcurrentHashMap<>();
        this.resultObjects = new ConcurrentHashMap<>();

        netManager.addEventListener(MethodInvokeMsg.METHOD_INVOCATION_EVENT, (event) -> {
            MethodInvokeMsg mim = (MethodInvokeMsg)event;
            if (mim.classID == classType.hashCode() && requestLatches.containsKey(mim.callID)) {
                CountDownLatch cdl = requestLatches.remove(mim.callID);
                if (mim.result != null)
                    resultObjects.put(mim.callID, mim.result);
                cdl.countDown();
            }
        });
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
        UUID callID = UUID.randomUUID();

        CountDownLatch cdl = new CountDownLatch(1);
        requestLatches.put(callID, cdl);
        MethodInvokeMsg invokeMsg = new MethodInvokeMsg(
                callID,
                classType.hashCode(),
                MethodInvokeMsg.getMethodID(method),
                arguments,
                null
        );

        netManager.dispatchEventAt(remoteNetDescriptor, invokeMsg);

        long responseTimeout = 5000;
        try {
            if (responseTimeout > 0) {
                // block the caller thread until we get some response...
                // ...but with a specified timeout to avoid indefinitely blocking of caller.
                cdl.await(responseTimeout, TimeUnit.MILLISECONDS);
            } else {
                cdl.await();
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        Object res = resultObjects.remove(callID);
        if (res instanceof Throwable)
            throw new IllegalStateException((Throwable) res);

        return res;
    }

    @SuppressWarnings("unchecked")
    public static <T> T create(NetManager netManager, MachineDescriptor remoteNetDescriptor, Class<?> clazz) throws Throwable {
        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                new Class<?>[]{clazz},
                new GlobalObjectProxy(netManager, remoteNetDescriptor, clazz)
        );
    }
}
