package de.tuberlin.pserver.runtime.filesystem;


import de.tuberlin.pserver.runtime.core.config.Config;
import de.tuberlin.pserver.runtime.core.diagnostics.MemoryTracer;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.lifecycle.Deactivatable;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFile;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFilePartitionScheduler;
import de.tuberlin.pserver.runtime.filesystem.local.LocalFile;
import de.tuberlin.pserver.runtime.filesystem.local.LocalFilePartitionScheduler;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import org.apache.commons.lang.ArrayUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class FileSystemManager implements Deactivatable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final String FILE_MASTER_NODE_ID = "worker.filesystem.masterID";

    public enum FileSystemType {

        LOCAL_FILE_SYSTEM,

        DISTRIBUTED_FILE_SYSTEM
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final int nodeID;

    private final Config config;

    private final FileSystemType type;

    private final Map<String,AbstractFile> files;

    private final InfrastructureManager infraManager;

    private final NetManager netManager;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public FileSystemManager(Config config, InfrastructureManager infraManager, NetManager netManager, FileSystemType type, int nodeID) {
        this.config         = config;
        this.infraManager   = infraManager;
        this.netManager     = netManager;
        this.type           = type;
        this.nodeID         = nodeID;
        this.files          = new HashMap<>();
    }

    // ---------------------------------------------------
    // Component Lifecycle.
    // ---------------------------------------------------

    public void clearContext() {

    }

    public void deactivate() {

    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public AbstractFile registerFile(DistributedTypeInfo typeInfo) {
        AbstractFile inputFile = (type == FileSystemType.DISTRIBUTED_FILE_SYSTEM) ?
                new DistributedFile(typeInfo) : new LocalFile(typeInfo);
        if (ArrayUtils.contains(typeInfo.nodes(), nodeID)) {
            files.put(typeInfo.input().filePath(), inputFile);
        }
        return inputFile;
    }

    public void buildPartitions() {

        System.out.println(MemoryTracer.getTrace("start_buildPartitions"));

        try {
            CountDownLatch rcvPartitionsLatch = new CountDownLatch(1);
            netManager.addEventListener(FilePartitionEvent.FILE_PARTITION_EVENT, event -> {
                FilePartitionEvent fpe = (FilePartitionEvent)event;
                files.get(fpe.filePartitionDescriptor.file).setFilePartition(fpe.filePartitionDescriptor);

                System.out.println("Received file partition descriptor.");

                rcvPartitionsLatch.countDown();
            });

            if (config.getInt(FILE_MASTER_NODE_ID) == nodeID) {

                for (AbstractFile inputFile : files.values()) {

                    List<AbstractFilePartition> filePartitions = (type == FileSystemType.DISTRIBUTED_FILE_SYSTEM) ?
                            new DistributedFilePartitionScheduler(config, infraManager, (DistributedFile) inputFile)
                                    .schedule(AbstractFilePartitionScheduler.ScheduleType.COLOCATED)
                            : new LocalFilePartitionScheduler((LocalFile) inputFile)
                                    .schedule(AbstractFilePartitionScheduler.ScheduleType.ORDERED);

                    for (AbstractFilePartition fpd : filePartitions) {

                        if (fpd.nodeID != nodeID) {
                            netManager.dispatchEventAt(
                                    new int[]{fpd.nodeID},
                                    new FilePartitionEvent(fpd)
                            );
                        } else
                            files.get(inputFile.getTypeInfo().input().filePath()).setFilePartition(fpd);
                    }
                }

                System.out.println(MemoryTracer.getTrace("after_scheduledFilePartitions"));

            } else {
                rcvPartitionsLatch.await();
            }

        } catch(Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public AbstractFileIterator getFileIterator(DistributedTypeInfo typeInfo) {
        AbstractFileIterator fileIterator = files.get(typeInfo.input().filePath()).getFileIterator();
        fileIterator.open();
        return fileIterator;
    }

    public AbstractFile getFile(DistributedTypeInfo typeInfo) {
        return files.get(typeInfo.input().filePath());
    }
}
