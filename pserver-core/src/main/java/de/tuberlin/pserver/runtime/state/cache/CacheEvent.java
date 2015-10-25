package de.tuberlin.pserver.runtime.state.cache;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.net.NetEvents;

public final class CacheEvent extends NetEvents.NetEvent {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String CACHE_PUSH_REQUEST_EVENT     = "CACHE_PUSH_REQUEST_EVENT";

    public static final String CACHE_PUSH_RESPONSE_EVENT    = "CACHE_PUSH_RESPONSE_EVENT";

    public static final String CACHE_PULL_REQUEST_EVENT     = "CACHE_PULL_REQUEST_EVENT";

    public static final String CACHE_PULL_RESPONSE_EVENT    = "CACHE_PULL_RESPONSE_EVENT";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long[] elementIndices;

    public final long[] elementValues;

    // ---------------------------------------------------
    // Constructors.
    // --------------------------------------------------

    public CacheEvent(final String type, final long[] elementIndices, final long[] elementValues) {

        super(type);

        this.elementIndices = Preconditions.checkNotNull(elementIndices);

        this.elementValues = Preconditions.checkNotNull(elementValues);
    }
}