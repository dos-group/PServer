package de.tuberlin.pserver.crdt.graphs;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

// TODO: this needs testing and debugging and major cleanup
public class AddOnlyDAGTestJob extends Program {

    @Unit(at = "0")
    public void test(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            AddOnlyDAG<Integer> addOnlyDAG = new AddOnlyDAG<>("one", 2, runtimeManager);

            Vertex<Integer> v1 = new Vertex<>(1, 1);
            Vertex<Integer> v2 = new Vertex<>(2, 2);
            Vertex<Integer> v3 = new Vertex<>(3, 3);

            addOnlyDAG.addVertex(v1);
            addOnlyDAG.addVertex(v2);
            addOnlyDAG.addVertex(v3);

            addOnlyDAG.addEdge(new Edge(v1, v2));
            addOnlyDAG.addEdge(new Edge(v1, v3));
            addOnlyDAG.addEdge(new Edge(v2, v3));
            addOnlyDAG.addEdge(new Edge(v3, v1));


            addOnlyDAG.finish();

            System.out.println("[DEBUG] Graph of node " + programContext.runtimeContext.nodeID + ": "
                    + "\n" + "  Vertices: " + addOnlyDAG.getVertices()
                    + "\n" + "  Edges: " + addOnlyDAG.getEdges());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + addOnlyDAG.getBuffer());
        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            AddOnlyDAG<Integer> addOnlyDAG = new AddOnlyDAG<>("one", 2, runtimeManager);

            Vertex<Integer> v1 = new Vertex<>(1, 1);
            Vertex<Integer> v2 = new Vertex<>(2, 2);
            Vertex<Integer> v3 = new Vertex<>(3, 3);

            addOnlyDAG.addVertex(v1);
            addOnlyDAG.addVertex(v2);
            addOnlyDAG.addVertex(v3);

            addOnlyDAG.addEdge(new Edge(v1, v2));
            addOnlyDAG.addEdge(new Edge(v1, v3));
            addOnlyDAG.addEdge(new Edge(v2, v3));
            addOnlyDAG.addEdge(new Edge(v3, v1));


            addOnlyDAG.finish();

            System.out.println("[DEBUG] Graph of node " + programContext.runtimeContext.nodeID + ": "
                    + "\n" + "  Vertices: " + addOnlyDAG.getVertices()
                    + "\n" + "  Edges: " + addOnlyDAG.getEdges());
            System.out.println("[DEBUG] Buffer of node " + programContext.runtimeContext.nodeID + ": "
                    + addOnlyDAG.getBuffer());
        });
    }

    public static void main(final String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", "2");
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(AddOnlyDAGTestJob.class)
                .done();
    }
}
