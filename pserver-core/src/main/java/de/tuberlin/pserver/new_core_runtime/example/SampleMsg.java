package de.tuberlin.pserver.new_core_runtime.example;


public final class SampleMsg {

    public String text;

    public double[] data;

    public SampleMsg() {

        final int size_mb = 1;

        final int num_elements = (int)((1048576.0 * size_mb) / Double.BYTES);

        data = new double[num_elements];
    }
}
