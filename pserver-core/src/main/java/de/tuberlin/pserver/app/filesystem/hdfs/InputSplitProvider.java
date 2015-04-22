package de.tuberlin.pserver.app.filesystem.hdfs;

import de.tuberlin.pserver.core.infra.MachineDescriptor;

public interface InputSplitProvider {

    public abstract InputSplit getNextInputSplit(final MachineDescriptor md);
}
