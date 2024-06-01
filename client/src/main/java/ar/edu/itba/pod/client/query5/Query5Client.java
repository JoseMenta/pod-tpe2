package ar.edu.itba.pod.client.query5;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query2Result;
import ar.edu.itba.pod.data.results.Query5Result;
import ar.edu.itba.pod.queries.query5.*;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

public class Query5Client extends QueryClient {

    private final IMap<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;

    private final IMap<String, Integer> auxMap;
    public Query5Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(Util.QUERY_5_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(Util.QUERY_5_NAMESPACE);
        this.auxMap = hazelcast.getMap(Util.QUERY_5_NAMESPACE + "-aux");
    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::getCode,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets( ){
        loadData(this.ticketPath,
                getMapper(),
                Ticket::getInfractionCode,
                i -> i,
                ticketsMap::put);
    }

    public SortedSet<Query5Result> executeJob() throws ExecutionException, InterruptedException {
        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<String,Ticket> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<String, Ticket> job = tracker.newJob(source);

        Map<String, Integer> aux = job
                .mapper(new Query5FirstMapper())
                .combiner(new Query5Combiner())
                .reducer(new Query5FirstReducer())
                .submit()
                .get();

       auxMap.putAll(aux);
       final KeyValueSource<String,Integer> secondSource = KeyValueSource.fromMap(auxMap);
       final Job<String,Integer> secondJob = tracker.newJob(secondSource);

       return secondJob
               .mapper(new Query5SecondMapper())
               .reducer(new Query5SecondReducer())
               .submit(new Query5Collator())
               .get();
    }

    @Override
    public void close() {
        Optional.ofNullable(this.auxMap).ifPresent(Map::clear);
        Optional.ofNullable(this.infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(this.ticketsMap).ifPresent(MultiMap::clear);
        super.close();
    }

    public static void main(String[] args) {

        try(Query5Client client = new Query5Client("query5")){

            //Load data
            client.loadInfractions();
            client.loadTickets();

            System.out.println("Started");
            //Execute job
            SortedSet<Query5Result> ans = client.executeJob();
            System.out.println("Ended");


            //Print results
            ans.forEach(System.out::println);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }
}
