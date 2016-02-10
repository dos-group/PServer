package de.tuberlin.pserver.runtime.state.matrix.rpc;

import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.core.remoteobj.MethodInvocationMsg;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.types.matrix.partitioner.MatrixPartitioner;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public final class GlobalStateMatrixProxy implements InvocationHandler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final NetManager netManager;

    private final Map<UUID, CountDownLatch> requestLatches;

    private final Map<UUID, Object> resultObjects;

    private final StateDescriptor stateDescriptor;

    private final MatrixPartitioner partitioner;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    private GlobalStateMatrixProxy(ProgramContext programContext,
                                   StateDescriptor stateDescriptor) {

        this.netManager         = programContext.runtimeContext.netManager;
        this.stateDescriptor    = stateDescriptor;
        this.requestLatches     = new ConcurrentHashMap<>();
        this.resultObjects      = new ConcurrentHashMap<>();

        this.partitioner = MatrixPartitioner.createPartitioner(
                stateDescriptor.partitionType,
                programContext.nodeID,
                stateDescriptor.atNodes,
                stateDescriptor.rows,
                stateDescriptor.cols
        );

        netManager.addEventListener(MethodInvocationMsg.METHOD_INVOCATION_EVENT + "_" + stateDescriptor.stateName, (event) -> {
            MethodInvocationMsg mim = (MethodInvocationMsg)event;
            if (mim.classID == stateDescriptor.stateType.hashCode() && requestLatches.containsKey(mim.callID)) {
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
        MethodInvocationMsg invokeMsg = new MethodInvocationMsg(
                stateDescriptor.stateName,
                callID,
                stateDescriptor.stateType.hashCode(),
                MethodInvocationMsg.getMethodID(method),
                arguments,
                null
        );

        matrixOperationDispatch(method, arguments, invokeMsg);

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

    private void matrixOperationDispatch(Method method, Object[] arguments, MethodInvocationMsg invokeMsg) {

        //
        // TODO: How to generalize this approach to all (partitioned) Matrix operations?
        // TODO: This is likely to require a complete redesign of the DistributedMatrix abstraction....brrr
        //

        switch (stateDescriptor.scope) {

            case SINGLETON:
            case REPLICATED: {
                netManager.dispatchEventAt(stateDescriptor.atNodes, invokeMsg);
            } break;

            case PARTITIONED: {
                if (("set".equals(method.getName()) || "get".equals(method.getName())) && method.getParameterCount() >= 2) {
                    int[] dstNode = {partitioner.getPartitionOfEntry((Long) arguments[0], (Long) arguments[1])};
                    netManager.dispatchEventAt(dstNode, invokeMsg);
                } else
                    throw new UnsupportedOperationException();
            } break;

            case LOGICALLY_PARTITIONED:
                throw new UnsupportedOperationException();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T create(ProgramContext programContext, StateDescriptor stateDescriptor) throws Exception {
        return (T) Proxy.newProxyInstance(
                stateDescriptor.stateType.getClassLoader(),
                new Class<?>[]{stateDescriptor.stateType},
                new GlobalStateMatrixProxy(programContext, stateDescriptor)
        );
    }
}
