package de.tuberlin.pserver.runtime.filesystem;


import java.io.InputStream;

public interface AbstractFileIterationContext {

    InputStream getInputStream();

    int readNext() throws Exception;
}
