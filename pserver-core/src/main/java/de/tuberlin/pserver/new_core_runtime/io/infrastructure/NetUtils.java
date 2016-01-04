package de.tuberlin.pserver.new_core_runtime.io.infrastructure;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public final class NetUtils {

    private static final Set<Integer> reservedPorts = new HashSet<>();

    public static InetAddress getIPAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

            while (interfaces.hasMoreElements()) {
                NetworkInterface curInterface = interfaces.nextElement();

                // Don't use the loopback interface.
                if (curInterface.isLoopback()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = curInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress curAddress = addresses.nextElement();

                    // Check only for IPv4 addresses.
                    if (curAddress instanceof Inet4Address) {
                        return curAddress;
                    }
                }
            }
        } catch (SocketException e) {
            throw new IllegalStateException(e.getCause());
        }

        final InetAddress address;
        try {
            address = InetAddress.getByName(InetAddress.getLocalHost().getHostName());
        } catch(Exception e) {
            throw new IllegalStateException(e);
        }

        return address;
    }

    public static int getFreePort() {
        int freePort = -1;
        do {
            try {
                final ServerSocket ss = new ServerSocket(0);
                freePort = ss.getLocalPort();
                ss.close();
            } catch (IOException e) {
            }
        } while (reservedPorts.contains(freePort) || freePort < 1024 || freePort > 65535);
        reservedPorts.add(freePort);
        return freePort;
    }
}
