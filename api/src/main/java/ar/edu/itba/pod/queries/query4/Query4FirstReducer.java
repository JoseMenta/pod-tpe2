package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.Map;

public class Query4FirstReducer implements ReducerFactory<String, String, Pair<String,Integer>> {

    /**
     * Given the (<neighbourhood, plate>, 1) pairs, returns the (<neighbourhood, plate>, total) pair
     * where total is the sum of ones (amount of pairs with same key)
     */
    @Override
    public Reducer<String, Pair<String,Integer>> newReducer(String key) {
        return new Query4Reducer();
    }

    private class Query4Reducer extends Reducer<String, Pair<String,Integer>> {

        transient Map<String, Integer> aux;

        @Override
        public void beginReduce() {
             this.aux = new HashMap<>();
        }

        @Override
        public void reduce(String plate) {
            aux.putIfAbsent(plate,0);
            aux.put(plate,aux.get(plate)+1);
        }

        @Override
        public Pair<String,Integer> finalizeReduce() {
            Map.Entry<String, Integer> entry = null;
            for(Map.Entry<String, Integer> auxEntry : aux.entrySet()) {
                if(entry == null || entry.getValue() < auxEntry.getValue()){
                    entry = auxEntry;
                }
            }
            return new Pair<>(entry.getKey(),entry.getValue());
        }
    }
}
