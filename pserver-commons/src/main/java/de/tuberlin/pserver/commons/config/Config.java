package de.tuberlin.pserver.commons.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface Config {

    boolean hasPath(final String path);

    boolean isEmpty();

    Number getNumber(final String path);

    boolean getBoolean(final String path);

    int getInt(final String path);

    long getLong(final String path);

    double getDouble(final String path);

    String getString(final String path);

    Config getObject(final String path);

    Config getConfig(final String path);

    Object getAnyRef(final String path);

    Long getBytes(final String path);

    long getDuration(final String path, final TimeUnit unit);

    List<Boolean> getBooleanList(final String path);

    List<Number> getNumberList(final String path);

    List<Integer> getIntList(final String path);

    List<Long> getLongList(final String path);

    List<Double> getDoubleList(final String path);

    List<String> getStringList(final String path);

    List<? extends Config> getObjectList(final String path);

    List<? extends Object> getAnyRefList(final String path);

    List<Long> getBytesList(final String path);

    List<Long> getDurationList(final String path, final TimeUnit unit);
}
