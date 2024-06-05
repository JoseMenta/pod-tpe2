package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query5Combiner implements CombinerFactory<String,Pair<Double, Integer>,Pair<Double, Integer>> {

    @Override
    public Combiner<Pair<Double, Integer>, Pair<Double, Integer>> newCombiner(String s) {
        return new Combiner<Pair<Double, Integer>, Pair<Double, Integer>>() {

            private double sum = 0;
            private int count = 0;

            @Override
            public void beginCombine() {
                this.sum = 0;
                this.count = 0;
            }

            @Override
            public void reset() {
                this.sum = 0;
                this.count = 0;
            }

            @Override
            public void combine(Pair<Double, Integer> pair) {
                this.sum+=pair.getFirst();
                this.count+=pair.getSecond();
            }

            @Override
            public Pair<Double, Integer> finalizeChunk() {
                return new Pair<>(this.sum, this.count);
            }
        };
    }
}
