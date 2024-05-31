package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query5Combiner implements CombinerFactory<String,Pair<Integer, Integer>,Pair<Integer, Integer>> {

    @Override
    public Combiner<Pair<Integer, Integer>, Pair<Integer, Integer>> newCombiner(String s) {
        return new Combiner<Pair<Integer, Integer>, Pair<Integer, Integer>>() {

            private int sum = 0;
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
            public void combine(Pair<Integer, Integer> pair) {
                this.sum+=pair.first();
                this.count+=pair.second();
            }

            @Override
            public Pair<Integer, Integer> finalizeChunk() {
                return new Pair<>(this.sum, this.count);
            }
        };
    }
}
