package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query5FirstMapper  implements Mapper<String, Double, String, Pair<Double, Integer>> {
    @Override
    public void map(String key, Double val, Context<String, Pair<Double, Integer>> context) {
        context.emit(key, new Pair<>(val, 1));
    }
}
