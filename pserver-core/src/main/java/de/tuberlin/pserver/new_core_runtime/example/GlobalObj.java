package de.tuberlin.pserver.new_core_runtime.example;


import de.tuberlin.pserver.new_core_runtime.io.network.NetManager;
import de.tuberlin.pserver.new_core_runtime.io.remoteobj.GlobalObject;

public class GlobalObj extends GlobalObject implements IGlobalObj<Integer, Double> {

    public GlobalObj(NetManager netManager) {
        super(netManager);
    }

    @Override
    public Double test(Integer i) {
        System.out.println("GlobalObj - test " + i);
        return i + 0.3;
    }
}
