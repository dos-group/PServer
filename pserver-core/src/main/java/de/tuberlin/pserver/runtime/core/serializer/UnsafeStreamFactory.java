package de.tuberlin.pserver.runtime.core.serializer;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;

import java.io.InputStream;
import java.io.OutputStream;


public class UnsafeStreamFactory implements StreamFactory {

    public Output createOutput(OutputStream os) {
        return new UnsafeOutput(os);
    }

    public Output createOutput(OutputStream os, int size) {
        return new UnsafeOutput(os, size);
    }

    public Output createOutput(int size, int limit) {
        return new UnsafeOutput(size, limit);
    }

    public Input createInput(InputStream os, int size) {
        return new UnsafeInput(os, size);
    }

    public Input createInput(byte[] buffer) {
        return new UnsafeInput(buffer);
    }
}
