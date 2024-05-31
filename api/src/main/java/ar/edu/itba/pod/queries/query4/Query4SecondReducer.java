package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query4SecondReducer implements ReducerFactory<String, Pair<String, Integer>, Pair<String, Integer>> {

    /**
     * Given the (neighbourhood, <plate, total>) pairs, returns the one that has the greater total
     */
    @Override
    public Reducer<Pair<String, Integer>, Pair<String, Integer>> newReducer(String s) {
        return new Query4Reducer();
    }

    private class Query4Reducer extends Reducer<Pair<String, Integer>, Pair<String, Integer>> {

        private Pair<String, Integer> mostTickets;

        @Override
        public void beginReduce() {
            this.mostTickets = null;
        }

        private boolean hasMoreTickets(Integer total) {
            return mostTickets == null || total > mostTickets.getSecond();
        }

        @Override
        public void reduce(Pair<String, Integer> plateTickets) {
            if (hasMoreTickets(plateTickets.getSecond())) {
                this.mostTickets = plateTickets;
            }
        }

        @Override
        public Pair<String, Integer> finalizeReduce() {
            return this.mostTickets;
        }
    }

}