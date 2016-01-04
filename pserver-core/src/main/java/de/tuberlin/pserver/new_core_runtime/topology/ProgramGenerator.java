package de.tuberlin.pserver.new_core_runtime.topology;


import de.tuberlin.pserver.new_core_runtime.io.network.NetDescriptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class ProgramGenerator {

    public static List<ProgramDescriptor> generate(List<NetDescriptor> nodes, ProgramGraph graph) {

        for (ProgramVertex v : graph.getVertices().values()) {
            v.data = nodes.remove(0);
        }

        List<ProgramDescriptor> pDescList = new ArrayList<>();
        breadthFirstTraversal(graph, (v) -> {
            List<NetDescriptor> in = new ArrayList<>();
            for (ProgramEdge inE : v.getInEdges())
                in.add((NetDescriptor)inE.src.data);
            List<NetDescriptor> out = new ArrayList<>();
            for (ProgramEdge outE : v.getOutEdges())
                out.add((NetDescriptor)outE.dst.data);
            pDescList.add(new ProgramDescriptor(v.vertexID, v.level, in, out));
        });

        return pDescList;
    }

    public static interface Visitor {
        public void visit(ProgramVertex v);
    }

    public static void breadthFirstTraversal(ProgramGraph graph, Visitor visitor) {
        Queue<ProgramVertex> queue = new LinkedList<>();
        queue.add(graph.root());
        visitor.visit(graph.root());
        graph.root().visited = true;
        while (!queue.isEmpty()) {
            ProgramVertex v = queue.remove();
            for (ProgramVertex next : getUnvisitedConnectedVertices(v)) {
                next.visited = true;
                visitor.visit(next);
                queue.add(next);
            }
        }
    }

    public static List<ProgramVertex> getUnvisitedConnectedVertices(ProgramVertex current) {
        final List<ProgramVertex> unvisitedVertices = new ArrayList<>();
        for (ProgramEdge inEdge : current.getInEdges())
            if (!inEdge.src.visited)
                unvisitedVertices.add(inEdge.src);
        for (ProgramEdge outEdge : current.getOutEdges())
            if (!outEdge.dst.visited)
                unvisitedVertices.add(outEdge.dst);
        return unvisitedVertices;
    }
}
