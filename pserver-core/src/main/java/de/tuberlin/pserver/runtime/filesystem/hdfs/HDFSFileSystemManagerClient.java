package de.tuberlin.pserver.runtime.filesystem.hdfs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.config.IConfig;
import de.tuberlin.pserver.runtime.core.events.IEventHandler;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.infra.MachineDescriptor;
import de.tuberlin.pserver.runtime.core.net.NetManager;
import de.tuberlin.pserver.runtime.core.net.RPCManager;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.FileFormat;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.runtime.state.partitioner.IMatrixPartitioner;
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

    private final Map<String,List<FileDataIterator<? extends Record>>> registeredIteratorMap;

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

    public void clearContext() {

        this.inputFileMap.clear();

        this.registeredIteratorMap.clear();
    }

    @Override
    public void deactivate() {
    }

    // ---------------------------------------------------
    // Public Method.
    // ---------------------------------------------------

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

        while(splitComputationLatch.getCount() > 0) {
            LOG.debug("waiting for hdfs-master to complete computation of input splits");
            try {
                splitComputationLatch.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {}
        }
        LOG.debug("hdfs-master completed computation of input splits");
        netManager.removeEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler);

        LOG.debug("initializing input splits");
        registeredIteratorMap.forEach(
                (k, v) -> v.forEach(FileDataIterator::initialize)
        );

        LOG.info("Input splits are computed and initialized.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Record> FileDataIterator<T> createFileIterator(final String filePath,
                                                                      final FileFormat fileFormat,
                                                                      final IMatrixPartitioner partitioner) {
        HDFSInputFile inputFile = inputFileMap.get(Preconditions.checkNotNull(filePath));
        if (inputFile == null) {
            inputFile = new HDFSInputFile(config, netManager, filePath, fileFormat);
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
