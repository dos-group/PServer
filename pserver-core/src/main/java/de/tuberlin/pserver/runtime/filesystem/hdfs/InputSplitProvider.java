package de.tuberlin.pserver.runtime.filesystem.hdfs;

import de.tuberlin.pserver.core.infra.MachineDescriptor;

public interface InputSplitProvider {

    public abstract InputSplit getNextInputSplit(final MachineDescriptor md);
}
