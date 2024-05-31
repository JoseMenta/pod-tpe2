package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query2FirstMapper implements Mapper<String, Ticket, Pair<String, String>,Integer> {
    @Override
    public void map(String s, Ticket ticket, Context<Pair<String, String>, Integer> context) {
        context.emit(new Pair<>(ticket.neighbourhood(),ticket.infractionCode()),1);
    }
}
