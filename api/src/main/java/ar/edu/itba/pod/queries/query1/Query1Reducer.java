package ar.edu.itba.pod.queries.query1;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query1Reducer implements ReducerFactory<String,Integer,Integer> {

    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new Reducer<Integer, Integer>() {

            private int sum = 0;

            @Override
            public void beginReduce() {
                this.sum = 0;
            }

            @Override
            public void reduce(Integer integer) {
                this.sum+=integer;
            }

            @Override
            public Integer finalizeReduce() {
                return sum;
            }
        };
    }
}
