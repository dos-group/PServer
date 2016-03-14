package de.tuberlin.pserver.crdt.graphs;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

// Immutable class
public class Vertex<T> implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int id;

    private final Set<Edge> incoming;

    private final Set<Edge> outgoing;

    private final T element;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Vertex(int id, T element) {

        this.id = id;

        this.incoming = new HashSet<>();

        this.outgoing = new HashSet<>();

        this.element = element;

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public boolean addIncoming(Edge e) {

        return incoming.add(e);

    }

    public Set<Edge> getIncoming() {

        return new HashSet<>(this.incoming);

    }

    public boolean addOutgoing(Edge e) {

        return outgoing.add(e);

    }

    public Set<Edge> getOutgoing() {

        return new HashSet<Edge>(this.outgoing);

    }

    public int getId() {

        return this.id;

    }

    public T getElement() {

        return this.element;

    }

    @Override
    public boolean equals(Object obj) {

        if(obj instanceof Vertex) {

            Vertex v = (Vertex) obj;

            return this.element.equals(v.getElement());

        }

        else {

            return false;

        }

    }

    @Override
    public int hashCode() {

        // TODO: hashCode must return equal values for equal objects (.equals)
        return element.hashCode();

    }

    @Override
    public String toString() {

        return "Vertex{" +
                "id=" + id +
                ", element=" + element +
                '}';

    }

}
