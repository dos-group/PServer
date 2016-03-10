package de.tuberlin.pserver.runtime.filesystem;


import java.io.InputStream;

public abstract class AbstractFileIterationContext {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public int row;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract InputStream getInputStream();

    public abstract int readNext() throws Exception;
}
