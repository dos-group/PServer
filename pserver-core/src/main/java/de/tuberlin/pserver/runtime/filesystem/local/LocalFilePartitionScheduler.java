package de.tuberlin.pserver.runtime.filesystem.local;


import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartition;
import de.tuberlin.pserver.runtime.filesystem.AbstractFilePartitionScheduler;
import de.tuberlin.pserver.types.typeinfo.DistributedTypeInfo;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

public class LocalFilePartitionScheduler implements AbstractFilePartitionScheduler {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final LocalFile file;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalFilePartitionScheduler(LocalFile file) { this.file = file; }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public List<AbstractFilePartition> schedule(ScheduleType type) {
        switch (type) {
            case ORDERED:
            case COLOCATED:
                return scheduler(file.getTypeInfo());
            default:
                throw new IllegalStateException();
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<AbstractFilePartition> scheduler(DistributedTypeInfo typeInfo) {
        final long totalLines = getNumberOfLines(typeInfo);
        List<AbstractFilePartition> inputPartitions = new ArrayList<>();
        // We assume here, that number of row-partitions determine file splitting. This is not necessarily
        // true i.e. if a line in a file contains row-values for one column. RecordFormatConfig should give
        // a hint for that.
        if(typeInfo.distributionScheme() != DistScheme.H_PARTITIONED) {
            inputPartitions.add(
                    new LocalFilePartition(
                            typeInfo.nodeId(),
                            typeInfo.input().filePath(),
                            typeInfo.input().fileFormat(),
                            0,
                            totalLines
                    )
            );
        } else {
            final long lps = totalLines / typeInfo.nodes().length;
            for (int i = 0; i < typeInfo.nodes().length; ++i) {
                final long linesToRead = (typeInfo.nodes()[i] == (typeInfo.nodes().length - 1))
                        ? lps + (totalLines % lps) : lps;
                long offset = linesToRead * typeInfo.nodes()[i];
                inputPartitions.add(
                        new LocalFilePartition(
                            typeInfo.nodes()[i],
                            typeInfo.input().filePath(),
                            typeInfo.input().fileFormat(),
                            offset,
                            linesToRead
                        )
                );
            }
        }
        return inputPartitions;
    }

    private long getNumberOfLines(DistributedTypeInfo typeInfo) {
        try {
            final LineNumberReader lnr = new LineNumberReader(new FileReader(typeInfo.input().filePath()));
            lnr.skip(Long.MAX_VALUE);
            final int numLines = lnr.getLineNumber();
            lnr.close();
            return  numLines; //+ 1;  // TODO: The first or the last line seems to be not counted...
        } catch(IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}
