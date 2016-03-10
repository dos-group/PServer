package de.tuberlin.pserver.runtime.filesystem.local;


import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;

import java.io.InputStream;

public class LocalFileIterationContext extends AbstractFileIterationContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public InputStream inputStream;

    public final LocalFilePartition partition;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public LocalFileIterationContext(LocalFilePartition partition) { this.partition = partition; }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public InputStream getInputStream() { return inputStream; }

    @Override
    public int readNext() throws Exception {
        return inputStream.read();
    }
}
