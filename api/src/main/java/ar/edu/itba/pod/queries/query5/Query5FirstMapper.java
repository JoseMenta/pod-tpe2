package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query5FirstMapper  implements Mapper<String, Integer, String, Pair<Integer, Integer>> {
    @Override
    public void map(String key, Integer val, Context<String, Pair<Integer, Integer>> context) {
        context.emit(key, new Pair<>(val, 1));
    }
}
