package ar.edu.itba.pod.client.query2;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query2Result;
import ar.edu.itba.pod.queries.query2.Query2FirstMapper;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Map;
import java.util.SortedSet;

import static ar.edu.itba.pod.Util.QUERY_2_NAMESPACE;


public class Query2Client extends QueryClient {

    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;

    public Query2Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(QUERY_2_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(QUERY_2_NAMESPACE);
    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::getCode,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets(){
        loadData(this.ticketPath,
                getMapper(),
                Ticket::getInfractionCode,
                i -> i,
                ticketsMap::put);
    }

//    public SortedSet<Query2Result> executeJob(){
//        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
//        final KeyValueSource<String,Ticket> source = KeyValueSource.fromMultiMap(ticketsMap);
//        final Job<String, Ticket> job = tracker.newJob(source);
//
//        Map<> job
//                .mapper(new Query2FirstMapper())
//                .red
//    }

    @Override
    public void close() {
        super.close();
    }

    public static void main(String[] args) {

        try(Query2Client client = new Query2Client("query2")){

            //Load data
            client.loadInfractions();
            client.loadTickets();
        }


    }
}