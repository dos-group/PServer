package de.tuberlin.pserver.commons.ds;

import java.util.ArrayList;
import java.util.Collection;

public final class Stack<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;

    public Stack(final int initialCapacity) { super(initialCapacity); }

    public Stack() { this(10); }

    public Stack(final Collection<T> collection) { super(collection); }

    public final void push(final T item) { add(item); }

    public final T pop() {
        final T top = peek();
        remove(size() - 1);
        return top;
    }

    public final T peek() {
        int size = size();
        if (size == 0) {
            return null;
        }
        return get(size - 1);
    }

    public final boolean empty() { return size() == 0; }

    public final int search(final T o) {
        int i = lastIndexOf(o);
        if (i >= 0) {
            return size() - i;
        }
        return -1;
    }
}







