package de.tuberlin.pserver.runtime.filesystem.hdfs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.core.net.RPCManager;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.config.AbstractRecordFormatConfig;
import de.tuberlin.pserver.types.PartitionType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class HDFSFileSystemManagerClient implements FileSystemManager, InputSplitProvider {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(HDFSFileSystemManagerClient.class);

    private final IConfig config;

    private final NetManager netManager;

    private final InputSplitProvider inputSplitProvider;

    private final Map<String,List<FileDataIterator<? extends IRecord>>> registeredIteratorMap;

    private final Map<String,HDFSInputFile> inputFileMap;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public HDFSFileSystemManagerClient(final IConfig config,
                                       final InfrastructureManager infraManager,
                                       final NetManager netManager,
                                       final RPCManager rpcManager) {

        Preconditions.checkNotNull(infraManager);
        Preconditions.checkNotNull(rpcManager);

        this.config     = Preconditions.checkNotNull(config);
        this.netManager = Preconditions.checkNotNull(netManager);

        final int hdfsMasterIdx = config.getInt("filesystem.hdfs.masterNodeIndex");
        final MachineDescriptor hdfsMasterMachine = infraManager.getMachine(hdfsMasterIdx);

        this.registeredIteratorMap = new HashMap<>();
        this.inputFileMap = new HashMap<>();

        this.inputSplitProvider = rpcManager.getRPCProtocolProxy(InputSplitProvider.class, hdfsMasterMachine);
    }

    @Override
    public void computeInputSplitsForRegisteredFiles() {
        final HDFSFileSystemManagerClient hfsmc = this;
        final CountDownLatch splitComputationLatch = new CountDownLatch(1);

        final IEventHandler handler = (e) -> {
            synchronized (hfsmc) {
                splitComputationLatch.countDown();
            }
        };

        netManager.addEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler);

        try {
            splitComputationLatch.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.debug("waiting for hdfs-master to complete computation of input splits");
        }
        netManager.removeEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler);

        registeredIteratorMap.forEach(
                (k, v) -> v.forEach(FileDataIterator::initialize)
        );

        LOG.info("Input splits are computed.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IRecord> FileDataIterator<T> createFileIterator(final String filePath,
                                                                      final AbstractRecordFormatConfig recordFormat,
                                                                      final PartitionType partitionType) {
        HDFSInputFile inputFile = inputFileMap.get(Preconditions.checkNotNull(filePath));
        if (inputFile == null) {
            inputFile = new HDFSInputFile(config, netManager, filePath, recordFormat);
            final Configuration conf = new Configuration();
            conf.set("fs.defaultFS", config.getString("filesystem.hdfs.url"));
            inputFile.configure(conf);
            inputFileMap.put(filePath, inputFile);
            registeredIteratorMap.put(filePath, new ArrayList<>());
        }
        final FileDataIterator<T> fileIterator = (FileDataIterator<T>)inputFile.iterator(this);
        registeredIteratorMap.get(filePath).add(fileIterator);
        return fileIterator;
    }

    @Override
    public InputSplit getNextInputSplit(final MachineDescriptor md) { return inputSplitProvider.getNextInputSplit(md); }
}
