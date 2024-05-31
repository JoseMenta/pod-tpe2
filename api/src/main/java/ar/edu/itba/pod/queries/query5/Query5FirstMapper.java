package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query5FirstMapper  implements Mapper<String, Ticket, String, Pair<Integer, Integer>> {
    @Override
    public void map(String s, Ticket ticket, Context<String, Pair<Integer, Integer>> context) {
        context.emit(ticket.getInfractionCode(), new Pair<>(ticket.getFineAmount(), 1));
    }
}
