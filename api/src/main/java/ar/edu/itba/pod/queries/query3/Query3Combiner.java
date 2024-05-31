package ar.edu.itba.pod.queries.query3;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query3Combiner implements CombinerFactory<String,Integer,Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(String s) {
        return new Combiner<Integer, Integer>() {

            private int sum = 0;

            @Override
            public void beginCombine() {
                this.sum = 0;
            }

            @Override
            public void reset() {
                this.sum = 0;
            }

            @Override
            public void combine(Integer integer) {
                this.sum+=integer;
            }

            @Override
            public Integer finalizeChunk() {
                return sum;
            }
        };
    }

}
