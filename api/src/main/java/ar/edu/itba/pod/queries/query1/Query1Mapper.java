package ar.edu.itba.pod.queries.query1;

import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query1Mapper implements Mapper<String, Ticket, String, Integer> {

    @Override
    public void map(String s, Ticket ticket, Context<String, Integer> context) {
        context.emit(ticket.infractionCode(),1);
    }
}
