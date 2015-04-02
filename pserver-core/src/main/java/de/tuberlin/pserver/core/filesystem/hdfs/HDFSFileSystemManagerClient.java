package de.tuberlin.pserver.core.filesystem.hdfs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.config.IConfig;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.FileSystemManager;
import de.tuberlin.pserver.core.infra.InfrastructureManager;
import de.tuberlin.pserver.core.infra.MachineDescriptor;
import de.tuberlin.pserver.core.net.NetManager;
import de.tuberlin.pserver.core.net.RPCManager;

public final class HDFSFileSystemManagerClient implements FileSystemManager, InputSplitProvider {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final IConfig config;

    private final NetManager netManager;

    private final InputSplitProvider inputSplitProvider;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public HDFSFileSystemManagerClient(final IConfig config,
                                       final InfrastructureManager infraManager,
                                       final NetManager netManager,
                                       final RPCManager rpcManager) {

        Preconditions.checkNotNull(infraManager);
        Preconditions.checkNotNull(rpcManager);

        this.config     = Preconditions.checkNotNull(config);
        this.netManager = Preconditions.checkNotNull(netManager);

        final int hdfsMasterIdx = config.getInt("filesystem.hdfs.masterNodeIndex");
        final MachineDescriptor hdfsMasterMachine = infraManager.getMachine(hdfsMasterIdx);
        this.inputSplitProvider = rpcManager.getRPCProtocolProxy(InputSplitProvider.class, hdfsMasterMachine);
    }

    @Override
    public void computeInputSplitsForRegisteredFiles() {
        throw new IllegalStateException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType) {
        return (FileDataIterator<T>) new HDFSFileDataIterator(config, netManager.getMachineDescriptor(), this, filePath, null);
    }

    @Override
    public InputSplit getNextInputSplit(final MachineDescriptor md) {
        return inputSplitProvider.getNextInputSplit(md);
    }
}
