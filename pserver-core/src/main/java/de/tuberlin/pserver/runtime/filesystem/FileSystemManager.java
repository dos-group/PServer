package de.tuberlin.pserver.runtime.filesystem;


import de.tuberlin.pserver.diagnostics.MemoryTracer;
import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.lifecycle.Deactivatable;
import de.tuberlin.pserver.runtime.core.network.NetEvent;
import de.tuberlin.pserver.runtime.core.network.NetManager;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFile;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFileIterator;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFilePartition;
import de.tuberlin.pserver.runtime.filesystem.distributed.DistributedFilePartitionScheduler;
import de.tuberlin.pserver.runtime.filesystem.local.LocalFile;
import de.tuberlin.pserver.runtime.filesystem.local.LocalFilePartitionScheduler;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import org.apache.commons.lang.ArrayUtils;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class FileSystemManager implements Deactivatable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String FILE_MASTER_NODE_ID = "worker.filesystem.masterID";

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

    private final BlockingQueue<AbstractBlock> remainingBlocks = new LinkedBlockingQueue<>();

    private final CountDownLatch rcvPartitionsLatch = new CountDownLatch(1);

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

        netManager.addEventListener(DistributedFileIterator.FILE_SYSTEM_COLLECT_BLOCKS, (event) -> {
            System.out.println("FILE_SYSTEM_COLLECT_BLOCKS => " + ((NetEvent)event).srcMachineID);
            remainingBlocks.addAll((List<AbstractBlock>)event.getPayload());
        });

        netManager.addEventListener(DistributedFileIterator.FILE_SYSTEM_BLOCK_REQUEST, (event) -> {
            System.out.println("FILE_SYSTEM_BLOCK_REQUEST => " + ((NetEvent)event).srcMachineID);
            AbstractBlock block;
            try {
                block = remainingBlocks.take();
            } catch (InterruptedException e) {
                throw new IllegalStateException();
            }
            //((NetEvent)event).netChannel.sendMsg(new NetEvent(DistributedFileIterator.FILE_SYSTEM_BLOCK_RESPONSE, block));
            netManager.dispatchEventAt(
                    ((NetEvent)event).srcMachineID,
                    new NetEvent(DistributedFileIterator.FILE_SYSTEM_BLOCK_RESPONSE, block)
            );
        });

        netManager.addEventListener(FilePartitionEvent.FILE_PARTITION_EVENT, event -> {
            System.out.println("Received file partition descriptor.");
            DistributedFilePartition dfp = (DistributedFilePartition)event.getPayload();
            files.get(dfp.file).setFilePartition(dfp);
            rcvPartitionsLatch.countDown();
        });
    }

    // ---------------------------------------------------
    // Component Lifecycle.
    // ---------------------------------------------------

    public void clearContext() {}

    public void deactivate() {}

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public AbstractFile getFile(DistributedTypeInfo typeInfo) { return files.get(typeInfo.input().filePath()); }

    public AbstractFile registerFile(DistributedTypeInfo typeInfo) {
        AbstractFile inputFile = (type == FileSystemType.DISTRIBUTED_FILE_SYSTEM) ?
                new DistributedFile(config, netManager, typeInfo) : new LocalFile(typeInfo);
        if (ArrayUtils.contains(typeInfo.nodes(), nodeID))
            files.put(typeInfo.input().filePath(), inputFile);
        return inputFile;
    }

    public void buildPartitions() {
        MemoryTracer.printTrace("start_buildPartitions");
        try {
            if (config.getInt(FILE_MASTER_NODE_ID) == nodeID) {
                for (AbstractFile inputFile : files.values()) {
                    List<AbstractFilePartition> filePartitions = schedulePartitions(inputFile);

                    System.out.println(" ---------------------------------------------------------------- ");
                    System.out.println(" |                   SEND PARTITIONS TO NODES                   | ");
                    System.out.println(" ---------------------------------------------------------------- ");

                    for (AbstractFilePartition fp : filePartitions) {
                        if (fp.nodeID != nodeID) {
                            NetEvent fpe = new NetEvent(FilePartitionEvent.FILE_PARTITION_EVENT);
                            fpe.setPayload(fp);
                            netManager.dispatchEventAt(new int[]{ fp.nodeID }, fpe);
                            System.out.println("Send partition to Node: " + fp.nodeID
                                    + " | blocks.size = " +  ((DistributedFilePartition)fp).blocks.size());
                        } else {
                            files.get(inputFile.getTypeInfo().input().filePath()).setFilePartition(fp);
                        }
                    }
                }
                MemoryTracer.printTrace("after_scheduledFilePartitions");
            } else {
                if (files.size() > 0) {
                    System.out.println("Waiting for partitions.");
                    rcvPartitionsLatch.await();
                    System.out.println("Received partitions.");
                }
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

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<AbstractFilePartition> schedulePartitions(AbstractFile inputFile) {
        List<AbstractFilePartition> filePartitions;
        switch (type) {
            case DISTRIBUTED_FILE_SYSTEM: {
                DistributedFilePartitionScheduler partitionScheduler =
                        new DistributedFilePartitionScheduler(
                                config,
                                infraManager,
                                (DistributedFile) inputFile
                        );
                filePartitions = partitionScheduler.schedule(AbstractFilePartitionScheduler.ScheduleType.COLOCATED);
                remainingBlocks.addAll(partitionScheduler.getRemainingBlocks());
            } break;
            case LOCAL_FILE_SYSTEM: {
                filePartitions = new LocalFilePartitionScheduler((LocalFile) inputFile)
                        .schedule(AbstractFilePartitionScheduler.ScheduleType.ORDERED);
            } break;
            default:
                throw new UnsupportedOperationException();
        }
        return filePartitions;
    }
}
