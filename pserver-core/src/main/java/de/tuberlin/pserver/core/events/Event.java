package de.tuberlin.pserver.core.events;

import com.google.common.base.Preconditions;

import java.io.Serializable;

public class Event implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    public String type;

    private Object payload;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Event(final String type) {
        this(type, null);
    }
    public Event(final String type, Object payload) {
        Preconditions.checkNotNull(type);
        this.type = type;
        setPayload(payload);
    }

    public void setPayload(final Object payload) {
        this.payload = payload;
    }

    public Object getPayload() {
        return this.payload;
    }

    @Override
    public String toString() {
        return type;
    }
}
