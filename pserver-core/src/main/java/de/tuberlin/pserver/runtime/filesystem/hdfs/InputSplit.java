package de.tuberlin.pserver.runtime.filesystem.hdfs;

import java.io.Serializable;

public interface InputSplit extends Serializable {

    public abstract int getSplitNumber();

    public abstract String[] getHostnames();
}
