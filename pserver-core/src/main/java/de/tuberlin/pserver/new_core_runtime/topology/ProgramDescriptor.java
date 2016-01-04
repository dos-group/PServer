package de.tuberlin.pserver.new_core_runtime.topology;

import de.tuberlin.pserver.new_core_runtime.io.network.NetDescriptor;

import java.util.List;

public final class ProgramDescriptor {

    public final int vertexID;

    public final int level;

    public final List<NetDescriptor> in;

    public final List<NetDescriptor> out;

    public ProgramDescriptor(int vertexID, int level, List<NetDescriptor> in, List<NetDescriptor> out) {

        this.vertexID = vertexID;

        this.level = level;

        this.in = in;

        this.out = out;
    }
}
