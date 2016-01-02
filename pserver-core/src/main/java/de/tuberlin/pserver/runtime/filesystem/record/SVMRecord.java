package de.tuberlin.pserver.runtime.filesystem.record;

import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.runtime.state.entries.ImmutableSVMEntry;
import de.tuberlin.pserver.runtime.state.entries.ReusableSVMEntry;
import de.tuberlin.pserver.runtime.state.entries.SVMEntry;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by Morgan K. Geldenhuys on 17.12.15.
 */
public class SVMRecord<V extends Number> implements Record<SVMEntry, ReusableSVMEntry> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private int target;
    private Map<Integer, V> attributes;
    private boolean isFetched;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SVMRecord(int target, Map<Integer, V> attributes) {
        this.target = target;
        this.attributes = attributes;
        this.isFetched = false;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public int getTarget() {
        return this.target;
    }

    public SVMRecord set(Tuple2<Integer, Map<Integer, V>> data) {
        this.target = data._1;
        this.attributes = data._2;
        this.isFetched = false;
        return this;
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public SVMEntry get(int i) {
        return this.get(i, null);
    }

    @Override
    public SVMEntry get(int i, ReusableSVMEntry reusable) {
        if (this.isFetched)
            throw new NoSuchElementException();

        this.isFetched = true;

        if (reusable == null)
            return new ImmutableSVMEntry(this.attributes);
        else
            return (SVMEntry) reusable.set(this.attributes);
    }

    @Override
    public boolean hasNext() {
        return this.isFetched;
    }

    @Override
    public SVMEntry next() {
        return this.get(0);
    }

    @Override
    public SVMEntry next(ReusableSVMEntry reusable) {
        return this.get(0, reusable);
    }

}