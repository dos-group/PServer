package de.tuberlin.pserver.runtime.filesystem.distributed;


import de.tuberlin.pserver.runtime.core.config.Config;
import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartition;
import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartitionScheduler;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class DistributedFilePartitionScheduler implements AbstractFilePartitionScheduler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Configuration hdfsConfig;

    private final DistributedFile file;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DistributedFilePartitionScheduler(Config config, DistributedFile file) {
        this.file = file;
        String hdfsHome = config.getString("worker.filesystem.hdfs.home");
        String hdfsURL = config.getString("filesystem.hdfs.url");
        this.hdfsConfig = new Configuration();
        this.hdfsConfig.set("fs.defaultFS", hdfsURL);
        this.hdfsConfig.set("HADOOP_HOME", hdfsHome);
        this.hdfsConfig.set("hadoop.home.dir", hdfsHome);
        System.setProperty("HADOOP_HOME", hdfsHome);
        System.setProperty("hadoop.home.dir", hdfsHome);
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

        return new ArrayList<>();
    }

    private List<AbstractFilePartition> orderedBlockScheduler(DistributedTypeInfo typeInfo) {

        List<AbstractFilePartition> inputPartitions = new ArrayList<>();

        try {

            long totalLength = 0;
            List<FileStatus> files = new ArrayList<>();
            Path hdfsPath = new Path(typeInfo.input().filePath());
            FileSystem fs = hdfsPath.getFileSystem(hdfsConfig);
            FileStatus inputStatus = fs.getFileStatus(hdfsPath);

            if (inputStatus.isDirectory()) {
                for (FileStatus dir : fs.listStatus(hdfsPath)) {
                    if (!dir.isDirectory() && acceptFile(dir)) {
                        files.add(dir);
                        totalLength += dir.getLen();
                    }
                }
            } else {
                files.add(inputStatus);
                totalLength += inputStatus.getLen();
            }

            long globalBlockOffset = 0;
            long partitionSize = totalLength / typeInfo.nodes().length;
            List<Pair<String, List<DistributedBlock>>> partitions = new ArrayList<>();

            for (final FileStatus file : files) {

                BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
                Arrays.sort(blockLocations, (o1, o2) -> {
                    if (o1.getOffset() < o2.getOffset())
                        return -1;
                    else if (o1.getOffset() > o2.getOffset())
                        return 1;
                    else
                        return 0;
                });

                List<DistributedBlock> blocksPerPartition = new ArrayList<>();
                for (BlockLocation block : blockLocations) {
                    boolean isSplitBlock = (globalBlockOffset + block.getLength()) / partitionSize > partitions.size();
                    blocksPerPartition.add(new DistributedBlock(globalBlockOffset, block, isSplitBlock));
                    if (isSplitBlock) {
                        partitions.add(Pair.of(file.getPath().getName(), blocksPerPartition));
                        blocksPerPartition = new ArrayList<>();
                        blocksPerPartition.add(new DistributedBlock(globalBlockOffset, block, true));
                    }
                    globalBlockOffset += block.getLength();
                }
            }

            for (int partitionID = 0; partitionID < typeInfo.nodes().length; ++partitionID) {
                inputPartitions.add(
                        new DistributedFilePartition(
                                hdfsConfig,
                                typeInfo.nodes()[partitionID],
                                partitions.get(partitionID).getKey(),
                                typeInfo.input().fileFormat(),
                                partitions.get(partitionID).getValue().get(0).globalOffset,
                                partitionSize,
                                partitions.get(partitionID).getValue()
                        )
                );
            }

        } catch(Exception ex) {
            throw new IllegalStateException(ex);
        }

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
