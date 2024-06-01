package ar.edu.itba.pod.queries.query3;

import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query3Mapper implements Mapper<String, Ticket, String, Integer> {

    @Override
    public void map(String s, Ticket ticket, Context<String, Integer> context) {
        context.emit(ticket.getAgency(), ticket.getFineAmount());
    }

}
