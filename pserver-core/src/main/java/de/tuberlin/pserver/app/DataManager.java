package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.app.dht.valuetypes.DoubleBufferValue;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.FileSystemManager;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.net.NetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class DataManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public interface MatrixMerger<T> {

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

    public IConfig getConfig() { return config; }

    // ---------------------------------------------------

    public <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType) {
        return fileSystemManager.createFileIterator(filePath, recordType);
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

    public <T extends DoubleBufferValue> void mergeMatrix(final T localMtx, final MatrixMerger<T> merger) {
        final Key k = localMtx.getKey();
        final List<Value> matrices = Arrays.asList(globalPull(k.name));
        Collections.sort(matrices,
                (Value o1, Value o2) -> ((Integer)o1.getValueMetadata()).compareTo(((Integer)o2.getValueMetadata()))
        );
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

    public DoubleBufferValue createLocalMatrix(final String name, final int rows, final int cols) {
        Preconditions.checkNotNull(name);
        final Key key = createLocalKeyWithName(name);
        final DoubleBufferValue m = new DoubleBufferValue(rows, cols, false);
        m.setValueMetadata(instanceID);
        dht.put(key, m);
        return m;
    }

    public DoubleBufferValue getLocalMatrix(final String name) {
        Preconditions.checkNotNull(name);
        Key localKey = null;
        final Set<Key> keys = dht.getKey(name);
        for (final Key k : keys) {
            if (k.getPartitionDescriptor(instanceID) != null) {
                localKey = k;
                break;
            }
        }
        return (DoubleBufferValue)dht.get(localKey)[0];
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
