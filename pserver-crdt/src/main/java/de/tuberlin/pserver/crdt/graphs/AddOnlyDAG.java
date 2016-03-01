package de.tuberlin.pserver.crdt.graphs;

import de.tuberlin.pserver.operations.Operation;
import de.tuberlin.pserver.runtime.driver.ProgramContext;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * This class uses 2P-Sets to store the vertices and edges = &gt; all the implications that come with that
 *  (I think: elements can only be added and removed once)
 */
// TODO: This implementation is not functional yet... :( => saved for later
public class AddOnlyDAG<T> extends AbstractGraph<T> {

    public AddOnlyDAG(String id, int noOfReplicas, ProgramContext programContext) {
        super(id, noOfReplicas, programContext);

        // Add left and right sentinels
        Vertex<T> leftSentinel = new Vertex<>(0, null);
        Vertex<T> rightSentinel = new Vertex<>(1, null);
        addVertex(leftSentinel);
        addVertex(rightSentinel);

        // Add default sentinel edge
        addEdge(new Edge(leftSentinel, rightSentinel));

        ready();
    }

    @Override
    public boolean addVertex(Vertex<T> v) {
        return vertices.add(v);
    }

    @Override
    public boolean removeVertex(Vertex v) {
        // TODO: throw an exception
        return false;
    }

    @Override
    public boolean addEdge(Edge e) {
        // TODO: check if this creates a cycle

        // Check if the vertices exist
        if(!vertices.contains(e.getSource()) || !vertices.contains(e.getSink())) {
            return false;
        }

        if(!makesGraphCyclic(e)) {

            if(edges.add(e)) {
                e.getSource().addOutgoing(e);
                e.getSink().addIncoming(e);
                return true;
            } else {
                return false;
            }
        } else {
            System.out.println("[DEBUG] Could not add " + e + " because it would make DAG cyclic.");
        }
        return false;
    }

    @Override
    public boolean removeEdge(Edge e) {
        // TODO: throw an exception
        return false;
    }

    private boolean makesGraphCyclic(Edge e) {
        // TODO: improve performance by checking corner-cases (i.e. first edge etc.)
        // TODO: implement

        // Check for back edge
       /* if(e.getSource().getIncoming().contains(new Edge(e.getSink(), e.getSource()))) {
            return true;
        }*/

        // If a topological sort exists, then the graph is a DAG
        return !hasTopologicalSorting(e);
    }

    // Depth-First search to determine if graph is cyclic
    private boolean hasTopologicalSorting(Edge newEdge) {
        Set<Edge> topoEdges = edges.getSet();
        Set<Vertex<T>> unmarked = vertices.getSet();
        Set<Vertex<T>> tmpMarked = new HashSet<>();
        Set<Vertex<T>> marked = new HashSet<>();
        topoEdges.add(newEdge);

        // Add the new edge
        // TODO: this works but it's ugly
        for(Vertex x : unmarked) {
            if(x.getId() == newEdge.getSource().getId()) {
                x.addOutgoing(newEdge);
            }
            else if(x.getId() == newEdge.getSink().getId()) {
                x.addIncoming(newEdge);
            }
        }


        while(!unmarked.isEmpty()) {
            Vertex v = unmarked.iterator().next();
            if(!visit(v, unmarked, tmpMarked, marked)) {
                return false;
            }
        }
        return true;
    }

    private boolean visit(Vertex v, Set<Vertex<T>> unmarked, Set<Vertex<T>> tmpMarked, Set<Vertex<T>> marked) {
        if(tmpMarked.contains(v)) { return false; }
        if(!marked.contains(v)) {
            tmpMarked.add(v);
            unmarked.remove(v);

            for(Edge e : ((Set<Edge>)v.getOutgoing())) {
                if(!visit(e.getSink(), unmarked, tmpMarked, marked)) {
                    return false;
                }
            }

            tmpMarked.remove(v);
            marked.add(v);
            return true;
        }
        return false;
    }

    // Approach alternative
    /*private boolean hasTopologicalSorting(Edge newEdge) {
        // Source: https://en.wikipedia.org/wiki/Topological_sorting#Algorithms
        List<Vertex> topoSorting = new LinkedList<>();
        Set<Vertex> noIncomingEdges = new HashSet<>();
        Set<Edge> topoEdges = edges.getSet();
        Set<Vertex<T>> topoVertices = vertices.getSet();
        topoEdges.add(newEdge);


        // Create set of all vertices with no incoming edges
        for(Vertex v : topoVertices) {
            if(v.getIncoming().isEmpty()) {
                noIncomingEdges.add(v);
            }
        }

        while(!noIncomingEdges.isEmpty()) {
            Vertex v = noIncomingEdges.iterator().next();
            noIncomingEdges.remove(v);
            topoSorting.add(v);

            System.out.println("Loop§");
            for(Edge e : (Set<Edge>)v.getOutgoing()) {
                // Remove edge
                topoEdges.remove(e);
                v.getOutgoing().remove(e);
                e.getSink().getIncoming().remove(e);

                if(e.getSink().getIncoming().isEmpty()) {
                    noIncomingEdges.add(e.getSink());
                }
            }
        }


        return topoEdges.isEmpty();
    }*/


    @Override
    protected boolean update(int srcNodeId, Operation op) {
        return false;
    }
}
