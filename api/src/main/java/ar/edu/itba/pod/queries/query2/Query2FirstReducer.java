package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2FirstReducer implements ReducerFactory<Pair<String, String>,Integer,Integer> {
    @Override
    public Reducer<Integer, Integer> newReducer(Pair<String, String> stringStringPair) {
        return new Reducer<Integer, Integer>() {

            private int sum = 0;

            @Override
            public void reduce(Integer integer) {
                this.sum += integer;
            }

            @Override
            public Integer finalizeReduce() {
                return sum;
            }
        };
    }
}
