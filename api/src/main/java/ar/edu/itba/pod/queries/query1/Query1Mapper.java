package ar.edu.itba.pod.queries.query1;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.time.LocalDateTime;

public class Query1Mapper implements Mapper<String, String, String, Integer> {

    @Override
    public void map(String key, String val, Context<String, Integer> context) {
        context.emit(val,1);
    }
}
