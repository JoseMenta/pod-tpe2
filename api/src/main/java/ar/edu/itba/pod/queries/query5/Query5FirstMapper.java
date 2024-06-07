package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query5FirstMapper  implements Mapper<LocalDateTime, Pair<String,Double>, String, Pair<Double, Integer>> {
    @Override
    public void map(LocalDateTime key, Pair<String,Double> val, Context<String, Pair<Double, Integer>> context) {
        context.emit(val.getFirst(), new Pair<>(val.getSecond(), 1));
    }
}
