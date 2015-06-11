package de.tuberlin.pserver.app;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.dht.DHT;
import de.tuberlin.pserver.app.dht.Key;
import de.tuberlin.pserver.app.dht.Value;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.FileSystemManager;
import de.tuberlin.pserver.app.types.DMatrixValue;
import de.tuberlin.pserver.app.types.DVectorValue;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.math.*;
import de.tuberlin.pserver.math.Vector;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public final class DataManager {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public interface Merger<T> {

        public abstract void merge(final T s, final T[] m);
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

    private final List<FileDataIterator<CSVRecord>> filesToLoad;

    private final Map<UUID, List<Serializable>> resultObjects;

    private final Map<Long, PServerContext> contextResolver;

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
        this.fileSystemManager  = fileSystemManager;
        this.dht                = Preconditions.checkNotNull(dht);
        this.instanceID         = infraManager.getInstanceID();
        this.filesToLoad        = new ArrayList<>();
        this.resultObjects      = new HashMap<>();
        this.contextResolver    = new ConcurrentHashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public IConfig getConfig() { return config; }

    // ---------------------------------------------------

    // Must be called from the specific execution context!
    public void registerJobContext(final PServerContext ctx) {
        contextResolver.put(Thread.currentThread().getId(), Preconditions.checkNotNull(ctx));
    }

    // ---------------------------------------------------

    public <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType) {
        return fileSystemManager != null ? fileSystemManager.createFileIterator(filePath, recordType) : null;
    }

    // ---------------------------------------------------

    public void loadDMatrix(final String filePath) {
        filesToLoad.add(createFileIterator(Preconditions.checkNotNull(filePath), null));
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

    // ---------------------------------------------------

    public void mergeMatrix(final Matrix localMtx, final Merger<Matrix> merger) {
        Preconditions.checkState(localMtx.getOwner() != null);
        final Key k = ((Value)localMtx.getOwner()).getKey();
        final List<Value> matrices = Arrays.asList(globalPull(k.name));
        Collections.sort(matrices,
                (Value o1, Value o2) -> ((Integer)o1.getValueMetadata()).compareTo(((Integer)o2.getValueMetadata()))
        );
        final Matrix[] ms = new Matrix[matrices.size()];
        for (int i = 0; i < matrices.size(); ++i)
            ms[i] = ((DMatrixValue)matrices.get(i)).matrix;
        merger.merge(localMtx, ms);
    }

    // ---------------------------------------------------

    public void mergeVector(final Vector localVec, final Merger<Vector> merger) {
        Preconditions.checkState(localVec.getOwner() != null);
        final Key k = ((Value)localVec.getOwner()).getKey();
        final List<Value> vectors = Arrays.asList(globalPull(k.name));
        Collections.sort(vectors,
                (Value o1, Value o2) -> ((Integer)o1.getValueMetadata()).compareTo(((Integer)o2.getValueMetadata()))
        );
        final Vector[] ms = new Vector[vectors.size()];
        for (int i = 0; i < vectors.size(); ++i)
            ms[i] = ((DVectorValue)vectors.get(i)).vector;
        merger.merge(localVec, ms);
    }

    // ---------------------------------------------------

    public Matrix createLocalMatrix(final String name, final int rows, final int cols)
    { return createLocalMatrix(name, rows, cols, DMatrix.MemoryLayout.ROW_LAYOUT); }
    public Matrix createLocalMatrix(final String name, final int rows, final int cols, final DMatrix.MemoryLayout layout) {
        Preconditions.checkNotNull(name);
        final Key key = createLocalKeyWithName(name);
        final DMatrixValue m = new DMatrixValue(rows, cols, false, layout);
        m.setValueMetadata(instanceID);
        dht.put(key, m);
        return m.matrix;
    }

    // ---------------------------------------------------

    public Vector createLocalVector(final String name, final int size, final Vector.VectorType type) {
        Preconditions.checkNotNull(name);
        final Key key = createLocalKeyWithName(name);
        final DVectorValue v = new DVectorValue(size, false, type);
        v.setValueMetadata(instanceID);
        dht.put(key, v);
        return v.vector;
    }

    // ---------------------------------------------------

    public Matrix.RowIterator threadPartitionedRowIterator(final Matrix matrix) {
        Preconditions.checkNotNull(matrix);
        final long systemThreadID = Thread.currentThread().getId();
        final PServerContext ctx = contextResolver.get(systemThreadID);
        final int rowBlock = (int)matrix.numRows() / ctx.perNodeParallelism;
        int end = (ctx.threadID * rowBlock + rowBlock - 1);
        end = (ctx.threadID == ctx.perNodeParallelism - 1) ?  end + (int)matrix.numRows() % ctx.perNodeParallelism : end;
        return matrix.rowIterator(ctx.threadID * rowBlock, end);
    }

    // ---------------------------------------------------

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
        final Value value = dht.get(localKey)[0];
        if (value instanceof DMatrixValue)
            return ((DMatrixValue)dht.get(localKey)[0]).matrix;
        else
            throw new IllegalStateException();
    }

    // ---------------------------------------------------

    public void setResults(final UUID jobUID, final List<Serializable> results) {
        Preconditions.checkNotNull(jobUID);
        Preconditions.checkNotNull(results);
        resultObjects.put(jobUID, results);
    }

    public List<Serializable> getResults(final UUID jobUID) {
        Preconditions.checkNotNull(jobUID);
        return resultObjects.get(jobUID);
    }

    // ---------------------------------------------------

    public void postProloguePhase(final PServerContext ctx) {
        Preconditions.checkNotNull(ctx);
        if (fileSystemManager != null) {
            if (ctx.threadID == 0) {
                fileSystemManager.computeInputSplitsForRegisteredFiles();
                loadFilesIntoDHT();
            }
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /*private void loadFilesIntoDHT() {
        for (final FileDataIterator<CSVRecord> fileIterator : filesToLoad) {
            double[] currentSegment = (double[]) MemoryManager.getMemoryManager().allocSegmentAs(double[].class);
            List<double[]> buffers = new ArrayList<>();
            buffers.add(currentSegment);
            int rows = 0, cols = -1, localIndex = 0;
            while (fileIterator.hasNext()) {
                final CSVRecord record = fileIterator.next();

                if (cols == -1)
                    cols = record.size();
                if (record.size() != cols)
                    throw new IllegalStateException("cols must always have size: " + cols + " but it has record.size = " + record.size());

                for (int i = 0; i < record.size(); ++i) {
                    if (localIndex == currentSegment.length - 1) {
                        currentSegment = (double[]) MemoryManager.getMemoryManager().allocSegmentAs(double[].class);
                        buffers.add(currentSegment);
                        localIndex = 0;
                    }
                    currentSegment[localIndex] = Double.parseDouble(record.get(i));
                    ++localIndex;
                }
                ++rows;
            }
            final String filename = Paths.get(fileIterator.getFilePath()).getFileName().toString();
            final Key key = createLocalKeyWithName(filename);
            final PagedDMatrixValue dBuf = new PagedDMatrixValue(rows, cols, buffers);
            dht.put(key, dBuf);
        }
    }*/

    private void loadFilesIntoDHT() {
        for (final FileDataIterator<CSVRecord> fileIterator : filesToLoad) {
            double[] data = new double[0];
            double[] currentSegment = new double[4096];

            int rows = 0, cols = -1, localIndex = 0;
            while (fileIterator.hasNext()) {
                final CSVRecord record = fileIterator.next();

                if (cols == -1)
                    cols = record.size();
                if (record.size() != cols)
                    throw new IllegalStateException("cols must always have size: " + cols + " but it has record.size = " + record.size());

                for (int i = 0; i < record.size(); ++i) {
                    if (localIndex == currentSegment.length - 1) {
                        data = ArrayUtils.addAll(data, currentSegment);
                        currentSegment = new double[4096];
                        localIndex = 0;
                    }
                    currentSegment[localIndex] = Double.parseDouble(record.get(i));
                    ++localIndex;
                }

                ++rows;
            }

            if (localIndex > 0)
                data = ArrayUtils.addAll(data, currentSegment); // TODO: We waste here a bit memory, if the last segment is not full...

            final String filename = Paths.get(fileIterator.getFilePath()).getFileName().toString();
            final Key key = createLocalKeyWithName(filename);
            final DMatrixValue dBuf = new DMatrixValue(rows, cols, data);
            dht.put(key, dBuf);
        }
    }

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
