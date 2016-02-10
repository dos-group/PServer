package de.tuberlin.pserver.types;

public class InternalData<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private T data;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public InternalData(T data) {
        set(data);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void set(T data) { this.data = data; }

    public T get() { return data; }
}
