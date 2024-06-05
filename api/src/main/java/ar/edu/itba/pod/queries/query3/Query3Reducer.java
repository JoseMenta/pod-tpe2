package ar.edu.itba.pod.queries.query3;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query3Reducer implements ReducerFactory<String,Double,Double> {

    @Override
    public Reducer<Double, Double> newReducer(String s) {
        return new Reducer<Double, Double>() {

            private double sum = 0;

            @Override
            public void beginReduce() {
                this.sum = 0;
            }

            @Override
            public void reduce(Double integer) {
                this.sum+=integer;
            }

            @Override
            public Double finalizeReduce() {
                return sum;
            }
        };
    }

}
