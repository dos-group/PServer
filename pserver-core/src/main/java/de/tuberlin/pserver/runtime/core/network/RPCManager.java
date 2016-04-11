package de.tuberlin.pserver.runtime.core.network;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.events.EventHandler;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class RPCManager {

    // ---------------------------------------------------
    // RPC Events.
    // ---------------------------------------------------

    public static final class RPCCallerRequestEvent extends NetEvent {

        private static final String RPC_REQUEST_EVENT = "rpc_request_event";

        public final UUID callUID;

        public final RPCManager.MethodSignature methodSignature;

        public RPCCallerRequestEvent() { this(null, null); }
        public RPCCallerRequestEvent(final UUID callUID,
                                     final RPCManager.MethodSignature methodSignature) {

            super(RPC_REQUEST_EVENT);
            this.callUID = callUID;
            this.methodSignature = methodSignature;
        }
    }

    // -----------------------------------------------------------------------------------------

    public static final class RPCCalleeResponseEvent extends NetEvent {

        private static final String RPC_RESPONSE_EVENT = "rpc_response_event";

        public final UUID callUID;

        public final Object result;

        public RPCCalleeResponseEvent() { this(null, null); }
        public RPCCalleeResponseEvent(final UUID callUID,
                                      final Object result) {

            super(RPC_RESPONSE_EVENT);
            this.callUID = callUID;
            this.result = result;
        }
    }


    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(RPCManager.class);

    private final NetManager netManager;

    private final Map<Pair<Class<?>, UUID>, Object> cachedProxies;

    private final ProtocolCalleeProxy calleeProxy;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public RPCManager(final NetManager netManager) {
        Preconditions.checkNotNull(netManager);

        this.netManager = netManager;

        this.cachedProxies = new HashMap<>();

        final RPCEventHandler rpcEventHandler = new RPCEventHandler();

        final String[] rpcEvents = {RPCCallerRequestEvent.RPC_REQUEST_EVENT, RPCCalleeResponseEvent.RPC_RESPONSE_EVENT};

        this.netManager.addEventListener(rpcEvents, rpcEventHandler);

        this.calleeProxy = new ProtocolCalleeProxy();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void registerRPCProtocol(final Object protocolImplementation, final Class<?> protocolInterface) {
        Preconditions.checkNotNull(protocolImplementation);
        Preconditions.checkNotNull(protocolInterface);
        calleeProxy.registerProtocol(protocolImplementation, protocolInterface);
    }

    @SuppressWarnings("unchecked")
    public <T> T getRPCProtocolProxy(final Class<T> protocolInterface, final MachineDescriptor dstMachine) {
        Preconditions.checkNotNull(protocolInterface);
        Preconditions.checkNotNull(dstMachine);
        final Pair<Class<?>, UUID> proxyKey =  (Pair)Pair.of(protocolInterface, dstMachine.machineID);
        T proxy = (T) cachedProxies.get(proxyKey);
        if (proxy == null) {
            proxy = ProtocolCallerProxy.createProtocolProxy(150000, dstMachine.machineID, protocolInterface, netManager);
            cachedProxies.put(proxyKey, proxy);
        }
        return proxy;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class MethodSignature implements Serializable {

        private static final long serialVersionUID = -1L;

        public final String className;

        public final String methodName;

        public final Class<?>[] argumentTypes;

        public final Object[] arguments;

        public final Class<?> returnType;

        public MethodSignature() { this(null, null, null, null, null); }
        public MethodSignature(String className, String methodName, Class<?>[] argumentTypes, Object[] arguments, Class<?> returnType) {
            this.className = className;
            this.methodName = methodName;
            if (arguments != null) {
                this.argumentTypes = argumentTypes;
                this.arguments = arguments;
            } else {
                this.argumentTypes = null;
                this.arguments = null;
            }
            this.returnType = returnType;
        }
    }

    // -----------------------------------------------------------------------------------------

    @SuppressWarnings("unused")
    private static final class ProtocolCallerProxy implements InvocationHandler {

        private final long responseTimeout; // 5000; // in ms

        private final UUID dstMachineID;

        private final NetManager netManager;

        private final static Map<UUID, CountDownLatch> callerTable = Collections.synchronizedMap(new HashMap<UUID, CountDownLatch>());

        private final static Map<UUID, Object> callerResultTable = Collections.synchronizedMap(new HashMap<UUID, Object>());

        public ProtocolCallerProxy(long responseTimeout, final UUID dstMachineID, final NetManager netManager) {
            Preconditions.checkNotNull(dstMachineID);
            Preconditions.checkNotNull(netManager);

            this.responseTimeout = responseTimeout;

            this.dstMachineID = dstMachineID;

            this.netManager = netManager;
        }

        @SuppressWarnings("unchecked")
        public static <T> T createProtocolProxy(final long responseTimeout,
                                                final UUID dstMachineID,
                                                final Class<T> protocolInterface,
                                                final NetManager netManager) {

            final ProtocolCallerProxy pc = new ProtocolCallerProxy(responseTimeout, dstMachineID, netManager);
            return (T) Proxy.newProxyInstance(protocolInterface.getClassLoader(), new Class[]{protocolInterface}, pc);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] methodArguments) throws Throwable {

            // check if all arguments implement serializable
            if (methodArguments != null) {
                int argumentIndex = 0;
                for (final Object argument : methodArguments) {
                    if (!(argument instanceof Serializable))
                        throw new IllegalStateException("argument [" + argumentIndex + "] is not instance of" + "<"
                                + Serializable.class.getCanonicalName() + ">");
                    ++argumentIndex;
                }
            }

            final MethodSignature methodInfo =
                    new MethodSignature(method.getDeclaringClass().getSimpleName(),
                            method.getName(),
                            method.getParameterTypes(),
                            methodArguments,
                            method.getReturnType());

            // every remote call is identified by a unique id. The id is used to
            // resolve the associated response from remote site.
            final UUID callUID = UUID.randomUUID();
            final CountDownLatch cdl = new CountDownLatch(1);
            callerTable.put(callUID, cdl);

            // push to server...
            netManager.dispatchEventAt(dstMachineID, new RPCCallerRequestEvent(callUID, methodInfo));

            try {
                if (responseTimeout > 0) {
                    // block the caller thread until we get some response...
                    // ...but with a specified timeout to avoid indefinitely blocking of caller.
                    cdl.await(responseTimeout, TimeUnit.MILLISECONDS);
                } else {
                    cdl.await();
                }
            } catch (InterruptedException e) {
                LOG.info(e.getLocalizedMessage());
            }

            // if is no result hasFree, then a response time-out happened...
            if (!callerResultTable.containsKey(callUID))
                throw new IllegalStateException("no result of remote call " + callUID + " hasFree");

            // result is allowed to be null -> void as return primitiveType.
            final Object result = callerResultTable.get(callUID);
            // clean up our tables.
            callerResultTable.remove(callUID);
            callerTable.remove(callUID);

            // TODO: should we pass a crashed call to the caller?
            if (result instanceof Throwable)
                throw new IllegalStateException((Throwable) result);

            return result;
        }

        public static void notifyCaller(final UUID callUID, final Object result) {
            Preconditions.checkNotNull(callUID);
            callerResultTable.put(callUID, result);
            final CountDownLatch cdl = callerTable.get(callUID);
            cdl.countDown();
        }
    }

    // -----------------------------------------------------------------------------------------

    private final class ProtocolCalleeProxy {

        private final Map<String, Object> calleeTable = new HashMap<>();

        public void registerProtocol(final Object protocolImplementation, final Class<?> protocolInterface) {
            calleeTable.put(protocolInterface.getSimpleName(), protocolImplementation);
        }

        public RPCCalleeResponseEvent callMethod(final UUID callUID, final MethodSignature methodInfo) {
            Preconditions.checkNotNull(callUID);
            Preconditions.checkNotNull(methodInfo);
            final Object protocolImplementation = calleeTable.get(methodInfo.className);
            if (protocolImplementation == null) {
                return new RPCCalleeResponseEvent(callUID, new IllegalStateException("found no protocol implementation"));
            }
            // Maybe we could do some caching of method signatures
            // on the callee site for frequent repeated calls...
            try {
                final Method method = protocolImplementation.getClass().getMethod(methodInfo.methodName, methodInfo.argumentTypes);
                final Object result = method.invoke(protocolImplementation, methodInfo.arguments);
                return new RPCCalleeResponseEvent(callUID, result);
            } catch (Exception e) {
                return new RPCCalleeResponseEvent(callUID, e);
            }
        }
    }

    // -----------------------------------------------------------------------------------------

    private final class RPCEventHandler extends EventHandler {

        private ExecutorService executor = Executors.newCachedThreadPool();

        @Handle(event = RPCCallerRequestEvent.class)
        private void handleRPCRequest(final RPCCallerRequestEvent event) {
            this.executor.execute(new Runnable() {

                @Override
                public void run() {
                    final RPCCalleeResponseEvent calleeMsg =
                            calleeProxy.callMethod(event.callUID, event.methodSignature);
                    netManager.dispatchEventAt(event.srcMachineID, calleeMsg);
                }
            });
        }

        @Handle(event = RPCCalleeResponseEvent.class)
        private void handleRPCResponse(final RPCCalleeResponseEvent event) {
            ProtocolCallerProxy.notifyCaller(event.callUID, event.result);
        }
    }
}
