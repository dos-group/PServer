package de.tuberlin.pserver.runtime.core.config;

import com.typesafe.config.ConfigObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TypesafeConfig implements Config {

    private final com.typesafe.config.Config delegate;

    protected TypesafeConfig(final com.typesafe.config.Config config) {
        this.delegate = config;
    }

    @Override
    public boolean hasPath(final String path) {
        return delegate.hasPath(path);
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public Number getNumber(final String path) {
        return delegate.getNumber(path);
    }

    @Override
    public boolean getBoolean(final String path) {
        return delegate.getBoolean(path);
    }

    @Override
    public int getInt(final String path) {
        return delegate.getInt(path);
    }

    @Override
    public long getLong(final String path) {
        return delegate.getLong(path);
    }

    @Override
    public double getDouble(final String path) {
        return delegate.getDouble(path);
    }

    @Override
    public String getString(final String path) {
        return delegate.getString(path);
    }

    @Override
    public Config getObject(final String path) {
        return new TypesafeConfig(delegate.getObject(path).toConfig());
    }

    @Override
    public Config getConfig(final String path) {
        return new TypesafeConfig(delegate.getConfig(path));
    }

    @Override
    public Object getAnyRef(final String path) {
        return delegate.getAnyRef(path);
    }

    @Override
    public Long getBytes(final String path) {
        return delegate.getBytes(path);
    }

    @Override
    public long getDuration(final String path, final TimeUnit unit) {
        return delegate.getDuration(path, unit);
    }

    @Override
    public List<Boolean> getBooleanList(final String path) {
        return delegate.getBooleanList(path);
    }

    @Override
    public List<Number> getNumberList(final String path) {
        return delegate.getNumberList(path);
    }

    @Override
    public List<Integer> getIntList(final String path) {
        return delegate.getIntList(path);
    }

    @Override
    public List<Long> getLongList(final String path) {
        return delegate.getLongList(path);
    }

    @Override
    public List<Double> getDoubleList(final String path) {
        return delegate.getDoubleList(path);
    }

    @Override
    public List<String> getStringList(final String path) {
        return delegate.getStringList(path);
    }

    @Override
    public List<? extends Config> getObjectList(final String path) {
        List<? extends ConfigObject> x = delegate.getObjectList(path);
        List<Config> y = new ArrayList<Config>(x.size());
        for (ConfigObject o : x) {
            y.add(new TypesafeConfig(o.toConfig()));
        }
        return y;
    }

    @Override
    public List<?> getAnyRefList(final String path) {
        return delegate.getAnyRefList(path);
    }

    @Override
    public List<Long> getBytesList(final String path) {
        return delegate.getBytesList(path);
    }

    @Override
    public List<Long> getDurationList(final String path, final TimeUnit unit) {
        return delegate.getDurationList(path, unit);
    }
}
