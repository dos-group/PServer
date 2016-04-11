package de.tuberlin.pserver.runtime.dht;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.UUID;

public abstract class DHTObject implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected UUID internalUID;

    protected transient DHTKey key;

    protected Serializable valueMetadata;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setKey(final DHTKey key) { this.key = Preconditions.checkNotNull(key); }

    public DHTKey getKey() { return key; }

    public void setInternalUID(final UUID uid) { this.internalUID = Preconditions.checkNotNull(uid); }

    public UUID getInternalUID() { return internalUID; }

    public void setValueMetadata(final Serializable valueMetadata) { this.valueMetadata = Preconditions.checkNotNull(valueMetadata); }

    public Serializable getValueMetadata() { return valueMetadata; }
}
