package de.tuberlin.pserver.core.infra;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.UUID;

public final class MachineDescriptor implements Serializable, Comparable<MachineDescriptor> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    private transient static Gson gson = new Gson();

    public final UUID machineID;

    public final InetAddress address;

    public final Integer port;

    public final String hostname;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public MachineDescriptor(final UUID machineID,
                             final InetAddress address,
                             final int port,
                             final String hostname) {

        Preconditions.checkArgument(port > 1024 && port < 65535);
        this.machineID  = Preconditions.checkNotNull(machineID);
        this.address    = Preconditions.checkNotNull(address);
        this.port       = port;
        this.hostname   = Preconditions.checkNotNull(hostname);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public int hashCode() {
        int result = machineID.hashCode();
        result = 31 * result + address.hashCode();
        result = 31 * result + port.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (other.getClass() != getClass())
            return false;
        if (!(machineID.equals(((MachineDescriptor) other).machineID)))
            return false;
        if (port != ((MachineDescriptor) other).port)
            return false;
        return true;
    }

    @Override
    public String toString() { return toJson(); }

    @Override
    public int compareTo(final MachineDescriptor o) { return machineID.compareTo(o.machineID); }

    public String toJson() { return gson.toJson(this); }
}