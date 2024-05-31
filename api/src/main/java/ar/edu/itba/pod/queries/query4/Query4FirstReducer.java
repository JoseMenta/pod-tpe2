package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4FirstReducer implements ReducerFactory<Pair<String, String>, Integer, Integer> {

    /**
     * Given the (<neighbourhood, plate>, 1) pairs, returns the (<neighbourhood, plate>, total) pair
     * where total is the sum of ones (amount of pairs with same key)
     */
    @Override
    public Reducer<Integer, Integer> newReducer(Pair<String, String> p) {
        return new Query4Reducer();
    }

    private class Query4Reducer extends Reducer<Integer, Integer> {
        private int sum = 0;

        @Override
        public void beginReduce() {
            this.sum = 0;
        }

        @Override
        public void reduce(Integer integer) {
            this.sum += integer;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }
}
