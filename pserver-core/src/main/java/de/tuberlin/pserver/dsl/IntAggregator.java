package de.tuberlin.pserver.dsl;


import de.tuberlin.pserver.runtime.SlotContext;

public class IntAggregator extends Aggregator<Integer> {

    public enum Operation {
        ADD, COUNT, MIN, MAX
    }

    public IntAggregator(final SlotContext sc, final Integer partialAgg) throws Exception {
        super(sc, partialAgg);
    }

    public Integer apply(final Operation op) throws Exception {
        switch (op) {
            case ADD:   return apply(pa -> pa.parallelStream().mapToInt(Integer::intValue).sum());
            case COUNT: return apply(pa -> (int)pa.parallelStream().mapToInt(Integer::intValue).count());
            case MIN:   return apply(pa -> pa.parallelStream().mapToInt(Integer::intValue).min().getAsInt());
            case MAX:   return apply(pa -> pa.parallelStream().mapToInt(Integer::intValue).max().getAsInt());
            default:    throw new IllegalStateException();
        }
    }
}
