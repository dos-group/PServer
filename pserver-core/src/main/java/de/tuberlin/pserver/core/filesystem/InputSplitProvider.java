package de.tuberlin.pserver.core.filesystem;

import de.tuberlin.pserver.core.filesystem.hdfs.InputSplit;
import de.tuberlin.pserver.core.infra.MachineDescriptor;

public interface InputSplitProvider {

    public abstract InputSplit getNextInputSplit(final MachineDescriptor md);
}
