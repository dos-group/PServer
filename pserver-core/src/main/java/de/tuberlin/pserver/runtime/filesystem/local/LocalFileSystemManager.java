package de.tuberlin.pserver.runtime.filesystem.local;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.core.events.Event;
import de.tuberlin.pserver.runtime.core.events.IEventHandler;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.FileDataIterator;
import de.tuberlin.pserver.runtime.filesystem.FileSystemManager;
import de.tuberlin.pserver.runtime.filesystem.records.Record;
import de.tuberlin.pserver.types.matrix.implementation.partitioner.AbstractMatrixPartitioner;
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

    private final Map<String,List<FileDataIterator<? extends Record>>> registeredIteratorMap;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocalFileSystemManager(final InfrastructureManager infraManager, final NetManager netManager) {
        this.infraManager = Preconditions.checkNotNull(infraManager);
        this.netManager = Preconditions.checkNotNull(netManager);
        this.inputFileMap = new HashMap<>();
        this.registeredIteratorMap = new HashMap<>();
    }

    public void clearContext() {
        this.inputFileMap.clear();
        this.registeredIteratorMap.clear();
    }

    @Override
    public void deactivate() {
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Record> FileDataIterator<T> createFileIterator(final ProgramContext programContext,
                                                                     final StateDescriptor stateDescriptor) {

        ILocalInputFile<?> inputFile = inputFileMap.get(Preconditions.checkNotNull(stateDescriptor.path));
        if (inputFile == null) {
            AbstractMatrixPartitioner partitioner = StateDescriptor.createMatrixPartitioner(programContext, stateDescriptor);
            inputFile = new LocalInputFile(stateDescriptor.path, stateDescriptor.fileFormat, partitioner);
            inputFileMap.put(stateDescriptor.path, inputFile);
            registeredIteratorMap.put(stateDescriptor.path, new ArrayList<>());
        }
        final FileDataIterator<T> fileIterator = (FileDataIterator<T>)inputFile.iterator();
        registeredIteratorMap.get(stateDescriptor.path).add(fileIterator);
        return fileIterator;
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

        netManager.broadcastEvent(new NetEvent(PSERVER_LFSM_COMPUTED_FILE_SPLITS, true));

        LOG.debug("["+infraManager.getNodeID()+"] Finished computing local input splits");

        while(splitComputationLatch.getCount() > 0) {
            LOG.debug("["+infraManager.getNodeID()+"] Waiting for other srcStateObjectNodes to finish computing input splits");
            try {
                splitComputationLatch.await(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {}
        }

        LOG.debug("["+infraManager.getNodeID()+"] All srcStateObjectNodes finished computing local input splits");

        netManager.removeEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler); // TODO: This can lead to a deadlock!
    }
}
