package de.tuberlin.pserver.core.filesystem.local;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.FileSystemManager;
import de.tuberlin.pserver.core.infra.InfrastructureManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class LocalFileSystemManager implements FileSystemManager {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final InfrastructureManager infraManager;

    private final Map<String,LocalInputFile<?>> inputFileMap;

    private final Map<String,List<FileDataIterator<?>>> registeredIteratorMap;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocalFileSystemManager(final InfrastructureManager infraManager) {
        this.infraManager = Preconditions.checkNotNull(infraManager);
        this.inputFileMap = new HashMap<>();
        this.registeredIteratorMap = new HashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void computeInputSplitsForRegisteredFiles() {

        inputFileMap.forEach(
                (k, v) -> v.computeLocalFileSection(
                        infraManager.getMachines().size(),
                        infraManager.getInstanceID()
                )
        );

        registeredIteratorMap.forEach(
                (k, v) -> v.forEach(FileDataIterator::initialize)
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> FileDataIterator<T> createFileIterator(final String filePath, final Class<T> recordType) {
        LocalInputFile<?> inputFile = inputFileMap.get(Preconditions.checkNotNull(filePath));
        if (inputFile == null) {
            inputFile = new LocalCSVInputFile(filePath);
            inputFileMap.put(filePath, inputFile);
            registeredIteratorMap.put(filePath, new ArrayList<>());
        }
        final FileDataIterator<T> fileIterator = (FileDataIterator<T>)inputFile.iterator();
        registeredIteratorMap.get(filePath).add(fileIterator);
        return fileIterator;
    }
}
