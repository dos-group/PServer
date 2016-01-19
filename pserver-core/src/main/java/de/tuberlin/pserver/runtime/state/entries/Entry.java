package de.tuberlin.pserver.runtime.state.entries;

import java.io.Serializable;

/**
 * Created by hegemon on 08.01.16.
 */
public interface Entry<V extends Number> extends Serializable {

    long getRow();

    long getCol();

    V getValue();

}
