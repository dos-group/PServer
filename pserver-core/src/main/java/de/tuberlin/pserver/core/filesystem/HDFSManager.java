package de.tuberlin.pserver.core.filesystem;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.filesystem.hdfs.FileInputSplit;
import de.tuberlin.pserver.core.filesystem.hdfs.InputSplit;
import de.tuberlin.pserver.core.filesystem.hdfs.InputSplitAssigner;
import de.tuberlin.pserver.core.filesystem.hdfs.LocatableInputSplitAssigner;
import de.tuberlin.pserver.core.filesystem.hdfs.in.CSVInputFormat;
import de.tuberlin.pserver.core.filesystem.hdfs.in.InputFormat;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.core.net.RPCManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class HDFSManager implements InputSplitProvider {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String HDFS_URL = "hdfs://wally190.cit.tu-berlin.de:45010/";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(HDFSManager.class);

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final RPCManager rpcManager;

    private final Map<Pair<UUID,String>, InputSplitAssigner> inputSplitAssignerMap;

    private final List<Pair<String, Class<?>[]>> registeredSources;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public HDFSManager(final InfrastructureManager infraManager,
                       final NetManager netManager,
                       final RPCManager rpcManager) {

        this.infraManager   = Preconditions.checkNotNull(infraManager);
        this.netManager     = Preconditions.checkNotNull(netManager);
        this.rpcManager     = Preconditions.checkNotNull(rpcManager);

        this.inputSplitAssignerMap = new ConcurrentHashMap<>();
        this.registeredSources = new ArrayList<>();

        rpcManager.registerRPCProtocol(this, InputSplitProvider.class);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void registerSource(final String filePath, final Class<?>[] fieldTypes) {
        Preconditions.checkNotNull(filePath);
        Preconditions.checkNotNull(fieldTypes);
        registeredSources.add(Pair.of(filePath, fieldTypes));
    }

    @SuppressWarnings("unchecked")
    public void computeInputSplits() {
        try {
            for (final Pair<String, Class<?>[]> source : registeredSources) {
                final Path path = new Path(source.getLeft());
                final InputFormat inputFormat = new CSVInputFormat(path, source.getRight());
                final Configuration conf = new Configuration();
                conf.set("fs.defaultFS", HDFS_URL);
                inputFormat.configure(conf);
                final InputSplit[] inputSplits = inputFormat.createInputSplits(infraManager.getMachines().size());
                final InputSplitAssigner inputSplitAssigner = new LocatableInputSplitAssigner((FileInputSplit[]) inputSplits);
                for (final MachineDescriptor md : infraManager.getMachines()) {
                    inputSplitAssignerMap.put(Pair.of(md.machineID, source.getLeft()), inputSplitAssigner);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------

    @Override
    public InputSplit getNextInputSplit(final MachineDescriptor md) {
        Preconditions.checkNotNull(md);
        final InputSplitAssigner inputSplitAssigner = inputSplitAssignerMap.get(md.machineID);
        return inputSplitAssigner.getNextInputSplit(md);
        //LOG.info("========================================> HDFSManager::getNextInputSplit(" + md + ")");
        //return null;
    }
}
