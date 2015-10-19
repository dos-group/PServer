package de.tuberlin.pserver.runtime.filesystem.local;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.events.Event;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.record.IRecord;
import de.tuberlin.pserver.runtime.filesystem.record.IRecordIteratorProducer;
import de.tuberlin.pserver.runtime.partitioning.partitioner.IMatrixPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class LocalFileSystemManager implements FileSystemManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemManager.class);

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final Map<String,ILocalInputFile<?>> inputFileMap;

    private final Map<String,List<FileDataIterator<? extends IRecord>>> registeredIteratorMap;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocalFileSystemManager(final InfrastructureManager infraManager, final NetManager netManager) {
        this.infraManager = Preconditions.checkNotNull(infraManager);
        this.netManager = Preconditions.checkNotNull(netManager);
        this.inputFileMap = new HashMap<>();
        this.registeredIteratorMap = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IRecord> FileDataIterator<T> createFileIterator(final String filePath,
                                                                      final IRecordIteratorProducer recordFormat,
                                                                      final IMatrixPartitioner partitioner) {

        ILocalInputFile<?> inputFile = inputFileMap.get(Preconditions.checkNotNull(filePath));
        if (inputFile == null) {
            inputFile = new LocalInputFile(filePath, recordFormat, partitioner);
            inputFileMap.put(filePath, inputFile);
            registeredIteratorMap.put(filePath, new ArrayList<>());
        }
        final FileDataIterator<T> fileIterator = (FileDataIterator<T>)inputFile.iterator();
        registeredIteratorMap.get(filePath).add(fileIterator);
        return fileIterator;
    }

    @Override
    public void clearContext() {
        this.inputFileMap.clear();
        this.registeredIteratorMap.clear();
    }

    @Override
    public void computeInputSplitsForRegisteredFiles() {

        final CountDownLatch splitComputationLatch = new CountDownLatch(infraManager.getMachines().size() - 1);

        // I don't trust lambdas with closures to local variables anymore ...
        IEventHandler handler = new IEventHandler() {
            @Override
            public void handleEvent(Event event) {
                splitComputationLatch.countDown();
            }
        };

        netManager.addEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler);

        inputFileMap.forEach(
                (k, v) -> v.computeLocalFileSection(
                        infraManager.getMachines().size(),
                        infraManager.getNodeID()
                )
        );

        registeredIteratorMap.forEach(
                (k, v) -> v.forEach(FileDataIterator::initialize)
        );

        netManager.broadcastEvent(new NetEvents.NetEvent(PSERVER_LFSM_COMPUTED_FILE_SPLITS, true));

        LOG.debug("["+infraManager.getNodeID()+"] Finished computing local input splits");

        while(splitComputationLatch.getCount() > 0) {
            LOG.debug("["+infraManager.getNodeID()+"] Waiting for other nodes to finish computing input splits");
            try {
                splitComputationLatch.await(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {}
        }

        LOG.debug("["+infraManager.getNodeID()+"] All nodes finished computing local input splits");

        netManager.removeEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler); // TODO: This can lead to a deadlock!
    }
}
