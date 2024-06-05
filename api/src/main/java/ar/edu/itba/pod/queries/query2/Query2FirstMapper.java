package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query2FirstMapper implements Mapper<String, String, Pair<String, String>,Integer> {
    @Override
    public void map(String key, String value, Context<Pair<String, String>, Integer> context) {
        //key: neighbourhood
        //value: infraction code
        context.emit(new Pair<>(key,value),1);
    }
}
