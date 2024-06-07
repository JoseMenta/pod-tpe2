package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query2FirstMapper implements Mapper<LocalDateTime, Pair<String,String>, Pair<String, String>,Integer> {
    @Override
    public void map(LocalDateTime key, Pair<String,String> value, Context<Pair<String, String>, Integer> context) {
        //key: timestamp
        //value: <neighbourhood, infraction code>
        context.emit(value,1);
    }
}
