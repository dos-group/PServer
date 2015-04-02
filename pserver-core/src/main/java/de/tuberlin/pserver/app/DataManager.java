package de.tuberlin.pserver.app;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.BufferValue;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.FileSystemManager;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.experimental.types.matrices.DenseDoubleMatrix;
import de.tuberlin.pserver.math.experimental.types.matrices.DenseMatrix;
import de.tuberlin.pserver.math.experimental.types.matrices.Matrix;
import de.tuberlin.pserver.math.experimental.types.vectors.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

public final class DataManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public interface MatrixMerger<T extends Matrix> {

        public abstract void merge(final T a, final T b);
    }

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static enum DataEventType {

        MATRIX_EVENT("MATRIX_EVENT"),

        VECTOR_EVENT("VECTOR_EVENT");

        public final String eventType;

        DataEventType(final String eventType) { this.eventType = eventType; }

        @Override public String toString() { return eventType; }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    private final IConfig config;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final FileSystemManager fileSystemManager;

    private final DHT dht;

    private final int instanceID;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DataManager(final IConfig config,
                       final InfrastructureManager infraManager,
                       final NetManager netManager,
                       final FileSystemManager fileSystemManager,
                       final DHT dht) {

        this.config             = Preconditions.checkNotNull(config);
        this.infraManager       = Preconditions.checkNotNull(infraManager);
        this.netManager         = Preconditions.checkNotNull(netManager);
        this.fileSystemManager  = Preconditions.checkNotNull(fileSystemManager);
        this.dht                = Preconditions.checkNotNull(dht);
        this.instanceID         = infraManager.getInstanceID();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public IConfig getConfig() {
        return config;
    }

    // ---------------------------------------------------

    public <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType) {
        return fileSystemManager.createFileIterator(filePath, recordType);
    }

    // ---------------------------------------------------

    public void addDataEventListener(final DataEventType eventType, final IEventHandler handler) {
        netManager.addEventListener(eventType.eventType, new IEventHandler() {
            @Override
            public void handleEvent(Event event) {
                handler.handleEvent(event);
            }
        });
    }

    public void push(final int instanceID, final Matrix m) { push(instanceID, DataEventType.MATRIX_EVENT, m); }
    public void push(final int instanceID, final Vector v) { push(instanceID, DataEventType.VECTOR_EVENT, v); }
    public void push(final int instanceID, final DataEventType type, final Serializable data) {
        Preconditions.checkArgument(instanceID != infraManager.getInstanceID());
        final NetEvents.NetEvent event = new NetEvents.NetEvent(type.eventType);
        event.setPayload(data);
        netManager.sendEvent(infraManager.getMachine(instanceID), event);
    }

    // ---------------------------------------------------

    public final Value[] globalPull(final String name) {
        Preconditions.checkNotNull(name);
        int idx = 0;
        final Set<Key> keys = dht.getKey(name);
        final Value[] values = new Value[keys.size()];
        for (final Key key : keys) {
            values[idx] = dht.get(key)[0];
            values[idx].setKey(key);
            ++idx;
        }
        return values;
    }

    public final void globalPush(final String name, final BufferValue value) {
        final Set<Key> keys = dht.getKey(name);
        for (final Key k : keys)
            dht.put(k, value);
    }

    public <T extends DenseMatrix> void mergeMatrix(final T localMtx, final MatrixMerger<T> merger) {
        final Key k = localMtx.getKey();
        final Value[] matrices = globalPull(k.name);
        for (final Value v : matrices) {
            final T remoteMtx = (T)v;
            if (!localMtx.getKey().internalUID.equals(remoteMtx.getKey().internalUID)) {
                merger.merge(localMtx, remoteMtx);
                LOG.info("on instance [" + instanceID + "] merged matrix " + localMtx.getKey().internalUID
                        + " with matrix " + remoteMtx.getKey().internalUID);
            }
        }
    }

    // ---------------------------------------------------

    public DenseDoubleMatrix createLocalMatrix(final String name, final int rows, final int cols) {
        Preconditions.checkNotNull(name);
        final Key key = createLocalKeyWithName(name);
        final DenseDoubleMatrix m = new DenseDoubleMatrix(rows, cols, Matrix.BlockLayout.ROW_LAYOUT);
        dht.put(key, m);
        return m;
    }

    public Matrix getLocalMatrix(final String name) {
        Preconditions.checkNotNull(name);
        Key localKey = null;
        final Set<Key> keys = dht.getKey(name);
        for (final Key k : keys) {
            if (k.getPartitionDescriptor(instanceID) != null) {
                localKey = k;
                break;
            }
        }
        return (Matrix)dht.get(localKey)[0];
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private UUID createLocalUID() {
        int id; UUID uid;
        do {
            uid = UUID.randomUUID();
            id = (uid.hashCode() & Integer.MAX_VALUE) % infraManager.getMachines().size();
        } while (id != infraManager.getInstanceID());
        return uid;
    }

    private Key createLocalKeyWithName(final String name) {
        final UUID localUID = createLocalUID();
        final Key key = Key.newKey(localUID, name, Key.DistributionMode.DISTRIBUTED);
        return key;
    }
}
