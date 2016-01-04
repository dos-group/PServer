package de.tuberlin.pserver.new_core_runtime.topology;


import de.tuberlin.pserver.new_core_runtime.io.network.NetDescriptor;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class Test {

    public static void main(String[] args) {

        List<NetDescriptor> ndList = new LinkedList<>();

        ndList.add(new NetDescriptor(UUID.randomUUID(), null, 8000, "127.0.0.1"));
        ndList.add(new NetDescriptor(UUID.randomUUID(), null, 8001, "127.0.0.2"));
        ndList.add(new NetDescriptor(UUID.randomUUID(), null, 8002, "127.0.0.3"));
        ndList.add(new NetDescriptor(UUID.randomUUID(), null, 8003, "127.0.0.4"));
        ndList.add(new NetDescriptor(UUID.randomUUID(), null, 8004, "127.0.0.5"));
        ndList.add(new NetDescriptor(UUID.randomUUID(), null, 8005, "127.0.0.6"));

        ProgramGraph graph = new ProgramGraph();

        ProgramVertex v0 = new ProgramVertex(0, 0);
        ProgramVertex v1 = new ProgramVertex(1, 0);
        ProgramVertex v2 = new ProgramVertex(2, 1);
        ProgramVertex v3 = new ProgramVertex(3, 0);
        ProgramVertex v4 = new ProgramVertex(4, 0);
        ProgramVertex v5 = new ProgramVertex(5, 1);

        ProgramEdge e0 = new ProgramEdge(v0, v2);
        ProgramEdge e1 = new ProgramEdge(v1, v2);
        ProgramEdge e2 = new ProgramEdge(v3, v5);
        ProgramEdge e3 = new ProgramEdge(v4, v5);
        ProgramEdge e4 = new ProgramEdge(v2, v5);

        graph.addVertex(v0);
        graph.addVertex(v1);
        graph.addVertex(v2);
        graph.addVertex(v3);
        graph.addVertex(v4);
        graph.addVertex(v5);

        graph.addEdge(e0);
        graph.addEdge(e1);
        graph.addEdge(e2);
        graph.addEdge(e3);
        graph.addEdge(e4);

        List<ProgramDescriptor> programList = ProgramGenerator.generate(ndList, graph);
    }
}
