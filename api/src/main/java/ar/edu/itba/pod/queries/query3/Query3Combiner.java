package ar.edu.itba.pod.queries.query3;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query3Combiner implements CombinerFactory<String,Double,Double> {

    @Override
    public Combiner<Double, Double> newCombiner(String s) {
        return new Combiner<Double, Double>() {

            private double sum = 0;

            @Override
            public void beginCombine() {
                this.sum = 0;
            }

            @Override
            public void reset() {
                this.sum = 0;
            }

            @Override
            public void combine(Double integer) {
                this.sum+=integer;
            }

            @Override
            public Double finalizeChunk() {
                return sum;
            }
        };
    }

}
