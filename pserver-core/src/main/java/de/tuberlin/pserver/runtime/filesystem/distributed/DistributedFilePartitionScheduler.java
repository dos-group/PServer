package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
import de.tuberlin.pserver.runtime.filesystem.AbstractBlock;
import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartition;
import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartitionScheduler;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.*;

public final class DistributedFilePartitionScheduler implements AbstractFilePartitionScheduler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Config config;

    private final Configuration hdfsConfig;

    private final InfrastructureManager infraManager;

    private final DistributedFile file;

    private final Map<String, MachineDescriptor> hostMD;

    private final List<AbstractBlock> remainingBlocks = new ArrayList<>();

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFilePartitionScheduler(Config config, InfrastructureManager infraManager, DistributedFile file) {
        this.config = config;
        this.infraManager = infraManager;
        this.file = file;
        String hdfsHome = config.getString("worker.filesystem.hdfs.home");
        String hdfsURL = config.getString("worker.filesystem.hdfs.url");
        this.hdfsConfig = new Configuration();
        this.hdfsConfig.set("fs.defaultFS", hdfsURL);
        this.hdfsConfig.set("HADOOP_HOME", hdfsHome);
        this.hdfsConfig.set("hadoop.home.dir", hdfsHome);
        System.setProperty("HADOOP_HOME", hdfsHome);
        System.setProperty("hadoop.home.dir", hdfsHome);
        this.hostMD = new HashMap<>();
        for (MachineDescriptor md : infraManager.getMachines())
            hostMD.put(md.hostname, md);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public List<AbstractBlock> getRemainingBlocks() { return remainingBlocks; }

    public List<AbstractFilePartition> schedule(ScheduleType type) {
        switch (type) {
            case ORDERED:
                return orderedBlockScheduler(file.getTypeInfo());
            case COLOCATED:
                return colocatedBlockScheduler(file.getTypeInfo());
            default:
                throw new IllegalStateException();
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<AbstractFilePartition> colocatedBlockScheduler(DistributedTypeInfo typeInfo) {
        List<AbstractFilePartition> inputPartitions = new ArrayList<>();
        try {
            List<FileStatus> files = new ArrayList<>();
            Path hdfsPath = new Path(typeInfo.input().filePath());
            FileSystem dfs = hdfsPath.getFileSystem(hdfsConfig);
            FileStatus inputStatus = dfs.getFileStatus(hdfsPath);
            if (inputStatus.isDirectory())
                for (FileStatus dir : dfs.listStatus(hdfsPath))
                    if (!dir.isDirectory() && acceptFile(dir))
                        files.add(dir);
            else
                files.add(inputStatus);

            System.out.println(" ---------------------------------------------------------------- ");
            System.out.println(" |                      Partition Scheduler                     | ");
            System.out.println(" ---------------------------------------------------------------- ");

            int numBlocks = 0;

            Map<String, List<DistributedBlock>> hostBlockMap = new HashMap<>();
            for (FileStatus file : files) {
                BlockLocation[] blockLocations = dfs.getFileBlockLocations(file, 0, file.getLen());
                Arrays.sort(blockLocations, (o1, o2) -> {
                    if (o1.getOffset() < o2.getOffset())
                        return -1;
                    else
                        if (o1.getOffset() > o2.getOffset())
                            return 1;
                        else
                            return 0;
                });
                System.out.println("File [" + file.getPath() + "] has " + blockLocations.length + " blocks.");
                int blockSeqCounter = 0;
                for (BlockLocation bl : blockLocations) {
                    for (String host : bl.getHosts()) {
                        List<DistributedBlock> hostBlockList = hostBlockMap.get(host);
                        if (hostBlockList == null) {
                            hostBlockList = new LinkedList<>();
                            hostBlockMap.put(host, hostBlockList);
                        }

                        DistributedBlock db =
                                new DistributedBlock(
                                    file.getPath().toString(),
                                    blockSeqCounter,
                                    bl.isCorrupt(),
                                    bl.getOffset(),
                                    bl.getLength(),
                                    -1,
                                    false
                                );

                        hostBlockList.add(db);
                        ++numBlocks;
                        //System.out.println("Block " + db.toString());
                    }
                    ++blockSeqCounter;
                }
            }
            System.out.println(" ---------------------------------------------------------------- ");
            System.out.println(" |                           Phase 1                            | ");
            System.out.println(" ---------------------------------------------------------------- ");

            Map<String, List<DistributedBlock>> hostAssignments = new HashMap<>();
            hostBlockMap.forEach((k,v) -> {
                System.out.println(k + " : " + v.size());
                hostAssignments.put(k, new LinkedList<>());
            });

            final int replicationFactor = 3;
            final int blocksPerHost = ((numBlocks / replicationFactor) / hostAssignments.size());

            System.out.println(" ---------------------------------------------------------------- ");
            System.out.println(" |                           Phase 2                            | ");
            System.out.println(" ---------------------------------------------------------------- ");

            Set<Pair<String,Long>> assignedBlocks = new HashSet<>();
            while (true) {
                for (String host : hostBlockMap.keySet()) {
                    List<DistributedBlock> hostBlockList = hostBlockMap.get(host);
                    if (!hostBlockList.isEmpty()) {
                        DistributedBlock fileBlock = hostBlockList.remove(hostBlockList.size() - 1);
                        if (hostBlockList.isEmpty())
                            System.out.println("Host " + host + " is scheduled.");

                        if (fileBlock.isCorrupt) {
                            System.out.println("BLOCK " + fileBlock.file + "[" + fileBlock.blockSeqID + "] is corrupt.");
                        }

                        if (!fileBlock.isCorrupt && assignedBlocks.add(Pair.of(fileBlock.file, fileBlock.offset))) {
                            //hostAssignments.get(host).add(fileBlock);
                            if (hostAssignments.get(host).size() < blocksPerHost)
                                hostAssignments.get(host).add(fileBlock);
                            else
                                remainingBlocks.add(fileBlock);
                        }
                    }
                }
                boolean isFinished = true;
                for (String host : hostBlockMap.keySet())
                    isFinished &= hostBlockMap.get(host).isEmpty();
                if (isFinished)
                    break;
            }

            for (String host : hostAssignments.keySet()) {
                List<DistributedBlock> hostBlockList = hostAssignments.get(host);
                for (int i = 0; hostBlockList.size() < blocksPerHost && i < remainingBlocks.size(); ++i) {
                    hostBlockList.add((DistributedBlock) remainingBlocks.remove(remainingBlocks.size() - 1));
                }
            }

            System.out.println("number of blocks = " + numBlocks);
            System.out.println("number of blocks without replication = " + (numBlocks / replicationFactor) + " | assigned blocks = " + assignedBlocks.size());
            System.out.println("number of blocks per host = " + blocksPerHost);
            System.out.println("number of remaining/unscheduled blocks = " + remainingBlocks.size());

            System.out.println(" ---------------------------------------------------------------- ");
            System.out.println(" |                           Phase 3                            | ");
            System.out.println(" ---------------------------------------------------------------- ");

            hostAssignments.forEach((host,blocks) -> {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < blocks.size(); ++i) {
                    DistributedBlock bl = blocks.get(i);
                    sb.append("B['").append("").append("']")
                            .append(i).append("(").append(bl.offset).append(",")
                            .append(bl.length).append(") | ");
                }
                System.out.println(host + " => " + sb.toString());
                MachineDescriptor md = hostMD.get(host);
                if (md != null) {
                    int nodeID = infraManager.getMachineIndex(md);
                    inputPartitions.add(
                            new DistributedFilePartition(
                                    config.getString("worker.filesystem.hdfs.home"),
                                    config.getString("worker.filesystem.hdfs.url"),
                                    nodeID,
                                    typeInfo.input().filePath(),
                                    typeInfo.input().fileFormat(),
                                    -1, -1,
                                    blocks
                            )
                    );
                } else
                    System.out.println("Blocks at host " + host + " could not be assigned.");
            });

        } catch(Exception ex) {
            throw new IllegalStateException(ex);
        }
        return inputPartitions;
    }

    // ---------------------------------------------------

    private List<AbstractFilePartition> orderedBlockScheduler(DistributedTypeInfo typeInfo) {
        return null;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private boolean acceptFile(FileStatus fileStatus) {
        final String name = fileStatus.getPath().getName();
        return !name.startsWith("_") && !name.startsWith(".");
    }
}
