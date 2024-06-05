package ar.edu.itba.pod.queries.query1;

import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query1Mapper implements Mapper<String, String, String, Integer> {

    @Override
    public void map(String key, String val, Context<String, Integer> context) {
        context.emit(key,1);
    }
}
