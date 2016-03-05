package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.runtime.core.config.Config;
import de.tuberlin.pserver.runtime.core.infra.InfrastructureManager;
import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;
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

    private final Configuration hdfsConfig;

    private final InfrastructureManager infraManager;

    private final DistributedFile file;

    private final Map<String, MachineDescriptor> hostMD;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFilePartitionScheduler(Config config, InfrastructureManager infraManager, DistributedFile file) {
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
            long totalLength = 0;
            List<FileStatus> files = new ArrayList<>();
            Path hdfsPath = new Path(typeInfo.input().filePath());
            FileSystem dfs = hdfsPath.getFileSystem(hdfsConfig);
            FileStatus inputStatus = dfs.getFileStatus(hdfsPath);
            if (inputStatus.isDirectory()) {
                for (FileStatus dir : dfs.listStatus(hdfsPath)) {
                    if (!dir.isDirectory() && acceptFile(dir)) {
                        files.add(dir);
                        totalLength += dir.getLen();
                    }
                }
            } else {
                files.add(inputStatus);
                totalLength += inputStatus.getLen();
            }
            //long partitionSize = totalLength / typeInfo.nodes().length;

            System.out.println(" ---------------------------------------------------------------- ");
            System.out.println(" |                      Partition Scheduler                     | ");
            System.out.println(" ---------------------------------------------------------------- ");

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
                        hostBlockList.add(new DistributedBlock(null, file.getPath().toString(), blockSeqCounter, bl));
                    }
                    ++blockSeqCounter;
                }
            }

            System.out.println(" ---------------------------------------------------------------- ");

            Map<String, List<DistributedBlock>> hostAssignments = new HashMap<>();
            hostBlockMap.forEach((k,v) -> {
                System.out.println(k + " : " + v.size());
                hostAssignments.put(k, new LinkedList<>());
            });

            System.out.println(" ---------------------------------------------------------------- ");


            int globalBlockCount = 0;
            for (List<DistributedBlock> blockPerHost : hostAssignments.values())
                globalBlockCount += blockPerHost.size();

            System.out.println("=> global block count = " + globalBlockCount);
            int blocksPerHost = (globalBlockCount / hostAssignments.size());
            System.out.println("=> block rest = " + (globalBlockCount % hostAssignments.size()));


            List<DistributedBlock> restBlocks = new ArrayList<>();

            System.out.println(" ---------------------------------------------------------------- ");

            Set<Pair<String,Long>> assignedBlocks = new HashSet<>();
            while (true) {
                for (String host : hostBlockMap.keySet()) {
                    List<DistributedBlock> hostBlockList = hostBlockMap.get(host);
                    if (!hostBlockList.isEmpty()) {
                        DistributedBlock fileBlock = hostBlockList.remove(hostBlockList.size() - 1);
                        if (hostBlockList.isEmpty())
                            System.out.println("Host " + host + " is scheduled.");
                        if (/*!fileBlock.blockLoc.isCorrupt() &&*/
                                assignedBlocks.add(Pair.of(fileBlock.file, fileBlock.blockLoc.getOffset()))) {

                            if (hostAssignments.get(host).size() < blocksPerHost)
                                hostAssignments.get(host).add(fileBlock);
                            else
                                restBlocks.add(fileBlock);
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
                List<DistributedBlock> hostBlockList = hostBlockMap.get(host);
                for (int i = 0; hostBlockList.size() < blocksPerHost && i < restBlocks.size(); ++i) {
                    hostBlockList.add(restBlocks.remove(restBlocks.size() - 1));
                }
            }

            System.out.println(" ---------------------------------------------------------------- ");

            hostAssignments.forEach((host,blocks) -> {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < blocks.size(); ++i) {
                    DistributedBlock bl = blocks.get(i);
                    sb.append("B['").append(bl.file).append("']")
                            .append(i).append("(").append(bl.blockLoc.getOffset()).append(",")
                            .append(bl.blockLoc.getLength()).append(") | ");
                }
                System.out.println(host + " => " + sb.toString());
                MachineDescriptor md = hostMD.get(host);
                if (md != null) {
                    int nodeID = infraManager.getMachineIndex(md);
                    inputPartitions.add(
                            new DistributedFilePartition(
                                    hdfsConfig,
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

            System.out.println(" ---------------------------------------------------------------- ");

        } catch(Exception ex) {
            throw new IllegalStateException(ex);
        }

        return inputPartitions;
    }

    // ---------------------------------------------------

    private List<AbstractFilePartition> orderedBlockScheduler(DistributedTypeInfo typeInfo) {
        List<AbstractFilePartition> inputPartitions = new ArrayList<>();
        return inputPartitions;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private boolean acceptFile(FileStatus fileStatus) {
        final String name = fileStatus.getPath().getName();
        return !name.startsWith("_") && !name.startsWith(".");
    }
}
