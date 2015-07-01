package de.tuberlin.pserver.app.filesystem.local;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.app.filesystem.FileDataIterator;
import de.tuberlin.pserver.app.filesystem.FileSystemManager;
import de.tuberlin.pserver.app.filesystem.record.IRecord;
import de.tuberlin.pserver.app.filesystem.record.RecordFormat;
import de.tuberlin.pserver.core.events.IEventHandler;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.net.NetEvents;
import de.tuberlin.pserver.core.net.NetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public final class LocalFileSystemManager implements FileSystemManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public static final long SYNC_TIME = 2000; // 2s to sync. all simulation workers.

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemManager.class);

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    private final Map<String,ILocalInputFile<?>> inputFileMap;

    private final Map<String,List<FileDataIterator<?>>> registeredIteratorMap;

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
    public void computeInputSplitsForRegisteredFiles() {
        final CountDownLatch splitComputationLatch = new CountDownLatch(infraManager.getMachines().size() - 1);
        final IEventHandler handler = (e) -> {
            synchronized (LocalFileSystemManager.this) {
                splitComputationLatch.countDown();
            }
        };
        netManager.addEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler);

        final long start = System.currentTimeMillis();

        inputFileMap.forEach(
                (k, v) -> v.computeLocalFileSection(
                        infraManager.getMachines().size(),
                        infraManager.getInstanceID()
                )
        );

        registeredIteratorMap.forEach(
                (k, v) -> v.forEach(FileDataIterator::initialize)
        );

        final long end = System.currentTimeMillis();

        // We need to sync. here in simulation mode.
        if (end - start < SYNC_TIME) {
            try {
                Thread.sleep(SYNC_TIME - (end - start));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        netManager.broadcastEvent(new NetEvents.NetEvent(PSERVER_LFSM_COMPUTED_FILE_SPLITS));

        try {
            splitComputationLatch.await();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }

        netManager.removeEventListener(PSERVER_LFSM_COMPUTED_FILE_SPLITS, handler);
        LOG.debug("Input splits are computed.");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IRecord> FileDataIterator<T> createFileIterator(final String filePath, final RecordFormat recordFormat) {
        ILocalInputFile<?> inputFile = inputFileMap.get(Preconditions.checkNotNull(filePath));
        if (inputFile == null) {
            inputFile = new LocalnputFile(filePath, recordFormat);
            inputFileMap.put(filePath, inputFile);
            registeredIteratorMap.put(filePath, new ArrayList<>());
        }
        final FileDataIterator<T> fileIterator = (FileDataIterator<T>)inputFile.iterator();
        registeredIteratorMap.get(filePath).add(fileIterator);
        return fileIterator;
    }
}
