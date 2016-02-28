package de.tuberlin.pserver.runtime.filesystem;


import java.util.List;

public interface AbstractFilePartitionScheduler {

    enum ScheduleType {

        ORDERED,

        COLOCATED
    }

    List<AbstractFilePartition> schedule(ScheduleType type);
}
