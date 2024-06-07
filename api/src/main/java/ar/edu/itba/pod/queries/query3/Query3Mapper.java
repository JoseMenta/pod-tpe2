package ar.edu.itba.pod.queries.query3;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query3Mapper implements Mapper<LocalDateTime, Pair<String, Double>, String, Double> {

    @Override
    public void map(LocalDateTime key, Pair<String, Double> value, Context<String, Double> context) {
        context.emit(value.getFirst(), value.getSecond());
    }

}
