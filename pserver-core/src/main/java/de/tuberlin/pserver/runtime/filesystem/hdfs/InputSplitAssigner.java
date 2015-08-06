package de.tuberlin.pserver.runtime.filesystem.hdfs;

import de.tuberlin.pserver.core.infra.MachineDescriptor;

public interface InputSplitAssigner {

    public InputSplit getNextInputSplit(final MachineDescriptor md);
}