package de.tuberlin.pserver.new_core_runtime.io.infrastructure;


import de.tuberlin.pserver.new_core_runtime.io.network.NetDescriptor;

import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NetDescriptorDirectory {

    private final Map<String, NetDescriptor> descriptorMap;

    public NetDescriptorDirectory() {
        this.descriptorMap = new ConcurrentHashMap<>();
    }

    public void add(String name, NetDescriptor netDescriptor) {
        descriptorMap.put(name, netDescriptor);
    }

    public NetDescriptor get(String name) {
        return descriptorMap.get(name);
    }

    private static final AtomicInteger portCounter = new AtomicInteger(8000);
    public static NetDescriptor createRandomLocalhostNetDescriptor() {
        InetAddress address = NetUtils.getIPAddress();
        int port = portCounter.incrementAndGet(); //NetUtils.getFreePort();
        return new NetDescriptor(UUID.randomUUID(), address, port, address.getHostName());
    }
}
