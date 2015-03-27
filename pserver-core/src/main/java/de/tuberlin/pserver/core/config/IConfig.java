package de.tuberlin.pserver.core.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface IConfig {

    public static enum Type {

        PSERVER_CLIENT("client"),

        PSERVER_NODE("node"),

        PSERVER_SIMULATION("simulation");

        protected final String name;

        private Type(final String name) { this.name = name; }
    }

    boolean hasPath(final String path);

    boolean isEmpty();

    Number getNumber(final String path);

    boolean getBoolean(final String path);

    int getInt(final String path);

    long getLong(final String path);

    double getDouble(final String path);

    String getString(final String path);

    IConfig getObject(final String path);

    IConfig getConfig(final String path);

    Object getAnyRef(final String path);

    Long getBytes(final String path);

    long getDuration(final String path, final TimeUnit unit);

    List<Boolean> getBooleanList(final String path);

    List<Number> getNumberList(final String path);

    List<Integer> getIntList(final String path);

    List<Long> getLongList(final String path);

    List<Double> getDoubleList(final String path);

    List<String> getStringList(final String path);

    List<? extends IConfig> getObjectList(final String path);

    List<? extends Object> getAnyRefList(final String path);

    List<Long> getBytesList(final String path);

    List<Long> getDurationList(final String path, final TimeUnit unit);
}
