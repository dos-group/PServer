package de.tuberlin.pserver.runtime.partitioning;

import java.util.Collection;

public interface IPartitioner {

    Collection<RemotePartition> getPartitions();

}
