package de.tuberlin.pserver.dsl.state;


public enum RemoteUpdate {

    NO_UPDATE,

    SIMPLE_MERGE_UPDATE,

    DELTA_MERGE_UPDATE,

    COLLECT_PARTITIONS_UPDATE
}
