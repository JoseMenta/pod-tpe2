package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query5FirstReducer  implements ReducerFactory<String, Pair<Integer, Integer>, Integer> {
    @Override
    public Reducer<Pair<Integer, Integer>, Integer> newReducer(String s) {
        return new Reducer<Pair<Integer, Integer>, Integer>() {

            private int sum = 0;
            private int count = 0;
            @Override
            public void beginReduce() {
                this.sum = 0;
                this.count = 0;
            }

            @Override
            public void reduce(Pair<Integer, Integer> pair) {
                this.sum+= pair.first();
                this.count+= pair.second();
            }

            @Override
            public Integer finalizeReduce() {
                return this.sum / this.count;
            }
        };
    }
}
