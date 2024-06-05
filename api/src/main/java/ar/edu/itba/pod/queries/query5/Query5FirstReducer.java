package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query5FirstReducer  implements ReducerFactory<String, Pair<Double, Integer>, Integer> {
    @Override
    public Reducer<Pair<Double, Integer>, Integer> newReducer(String s) {
        return new Reducer<Pair<Double, Integer>, Integer>() {

            private double sum = 0;
            private int count = 0;
            @Override
            public void beginReduce() {
                this.sum = 0;
                this.count = 0;
            }

            @Override
            public void reduce(Pair<Double, Integer> pair) {
                this.sum+= pair.getFirst();
                this.count+= pair.getSecond();
            }

            @Override
            public Integer finalizeReduce() {
                return (int) (this.sum / this.count);
            }
        };
    }
}
