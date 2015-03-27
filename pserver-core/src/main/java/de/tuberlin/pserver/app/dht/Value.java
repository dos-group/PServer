package de.tuberlin.pserver.app.dht;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.UUID;

public abstract class Value implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected UUID internalUID;

    protected transient Key key;

    protected Serializable valueMetadata;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setKey(final Key key) { this.key = Preconditions.checkNotNull(key); }

    public Key getKey() { return key; }

    public void setInternalUID(final UUID uid) { this.internalUID = Preconditions.checkNotNull(uid); }

    public UUID getInternalUID() { return internalUID; }

    public void setValueMetadata(final Serializable valueMetadata) {
        this.valueMetadata = Preconditions.checkNotNull(valueMetadata);
    }

    public Serializable getValueMetadataI() { return valueMetadata; }
}
