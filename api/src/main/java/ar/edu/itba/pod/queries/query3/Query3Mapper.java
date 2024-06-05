package ar.edu.itba.pod.queries.query3;

import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query3Mapper implements Mapper<String, Double, String, Double> {

    @Override
    public void map(String key, Double value, Context<String, Double> context) {
        context.emit(key, value);
    }

}
