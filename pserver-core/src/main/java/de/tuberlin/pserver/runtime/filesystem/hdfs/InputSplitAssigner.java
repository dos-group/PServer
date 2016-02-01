package de.tuberlin.pserver.runtime.filesystem.hdfs;

import de.tuberlin.pserver.runtime.core.network.MachineDescriptor;

public interface InputSplitAssigner {

    public InputSplit getNextInputSplit(final MachineDescriptor md);
}