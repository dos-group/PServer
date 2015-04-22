package de.tuberlin.pserver.app.filesystem.hdfs;

import java.io.Serializable;

public interface InputSplit extends Serializable {

    public abstract int getSplitNumber();

    public abstract String[] getHostnames();
}
