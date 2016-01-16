package de.tuberlin.pserver.runtime.core.events;

import com.google.common.base.Preconditions;

import java.io.Serializable;

public class Event implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    public String type;

    private Object payload;

    public final boolean isSticky;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Event() { this(null, null, false); }
    public Event(final String type) { this(type, null, false); }
    public Event(final String type, final boolean isSticky) { this(type, null, isSticky); }
    public Event(final String type, Object payload) { this(type, payload, false); }
    public Event(final String type, Object payload, final boolean isSticky) {
        //Preconditions.checkNotNull(type);
        this.type = type;
        this.isSticky = isSticky;
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
