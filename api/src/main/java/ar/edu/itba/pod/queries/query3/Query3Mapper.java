package ar.edu.itba.pod.queries.query3;

import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query3Mapper implements Mapper<String, Integer, String, Integer> {

    @Override
    public void map(String key, Integer value, Context<String, Integer> context) {
        context.emit(key, value);
    }

}
