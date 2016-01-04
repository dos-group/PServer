package de.tuberlin.pserver.new_core_runtime.example;


import de.tuberlin.pserver.new_core_runtime.io.infrastructure.NetDescriptorDirectory;
import de.tuberlin.pserver.new_core_runtime.io.network.NetManager;

public final class Node0 {

    public static void main(String[] args) throws Exception {

        NetDescriptorDirectory netDescriptorDirectory = new NetDescriptorDirectory();
        netDescriptorDirectory.add("node_0", NetDescriptorDirectory.createRandomLocalhostNetDescriptor());
        netDescriptorDirectory.add("node_1", NetDescriptorDirectory.createRandomLocalhostNetDescriptor());

        NetManager netManager = new NetManager(netDescriptorDirectory.get("node_0"), 2);

        new Thread(() -> {

            try {

                netManager.start();

            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();

        new GlobalObj(netManager);
    }
}
