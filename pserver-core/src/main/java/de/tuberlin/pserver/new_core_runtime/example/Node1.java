package de.tuberlin.pserver.new_core_runtime.example;


import de.tuberlin.pserver.new_core_runtime.io.infrastructure.NetDescriptorDirectory;
import de.tuberlin.pserver.new_core_runtime.io.network.NetManager;
import de.tuberlin.pserver.new_core_runtime.io.remoteobj.GlobalObjectProxy;

public final class Node1 {

    public static void main(String[] args) throws Exception {

        NetDescriptorDirectory netDescriptorDirectory = new NetDescriptorDirectory();
        netDescriptorDirectory.add("node_0", NetDescriptorDirectory.createRandomLocalhostNetDescriptor());
        netDescriptorDirectory.add("node_1", NetDescriptorDirectory.createRandomLocalhostNetDescriptor());

        NetManager netManager = new NetManager(netDescriptorDirectory.get("node_1"), 2);

        new Thread(() -> {

            try {

                netManager.start();

            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();

        netManager.connect(netDescriptorDirectory.get("node_0"));

        try {
            IGlobalObj<Integer, Double> go = GlobalObjectProxy.create(netManager, netDescriptorDirectory.get("node_0"), IGlobalObj.class);

            double ret = go.test(55);


            System.out.println("ret = " + ret);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
