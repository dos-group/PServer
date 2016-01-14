package de.tuberlin.pserver.runtime.core.serializer;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.InputStream;
import java.io.OutputStream;

public interface StreamFactory {

    public Output createOutput(OutputStream os);

    public Output createOutput(OutputStream os, int size);

    public Output createOutput(int size, int limit);

    public Input createInput(InputStream os, int size);

    public Input createInput(byte[] buffer);
}
