package de.tuberlin.pserver.math.matrix32.entries;


public interface ReusableEntry32 extends Entry32 {

    ReusableEntry32 set(long row, long col, float value);
}
