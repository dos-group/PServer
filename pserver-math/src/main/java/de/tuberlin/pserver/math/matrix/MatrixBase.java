package de.tuberlin.pserver.math.matrix;


import de.tuberlin.pserver.math.SharedObject;

public interface MatrixBase extends SharedObject{

    public long rows();

    public long cols();

    public long sizeOf();

    public void lock();

    public void unlock();

    public void setOwner(final Object owner);

    public Object getOwner();
}
