package de.tuberlin.pserver.runtime.filesystem;

import de.tuberlin.pserver.compiler.StateDescriptor;
import de.tuberlin.pserver.runtime.core.common.Deactivatable;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.filesystem.records.Record;

public interface FileSystemManager extends Deactivatable {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    String PSERVER_LFSM_COMPUTED_FILE_SPLITS  = "PSERVER_LFSM_COMPUTED_FILE_SPLITS";

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    void computeInputSplitsForRegisteredFiles();

    <T extends Record> FileDataIterator<T> createFileIterator(
            final ProgramContext programContext,
            final StateDescriptor stateDescriptor);

    void clearContext();
}
