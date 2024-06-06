package ar.edu.itba.pod.client.query2;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query2Result;
import ar.edu.itba.pod.queries.query2.*;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static ar.edu.itba.pod.Util.QUERY_2_NAMESPACE;


public class Query2Client extends QueryClient {

    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<Integer, Pair<String,String>> ticketsMap;

    private final IMap<Pair<String,String>,Integer> auxMap;

    public Query2Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(QUERY_2_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(QUERY_2_NAMESPACE);
        this.auxMap = hazelcast.getMap(QUERY_2_NAMESPACE + "-aux");
    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                (index, val)->val.getCode(),
                i->i,
                infractionsMap::put);
    }

    private void loadTickets(){
        loadData(this.ticketPath,
                getMapper(),
                (index,val)->index,
                v -> new Pair<>(v.getNeighbourhood(),v.getInfractionCode()),
                ticketsMap::put);
    }

    public SortedSet<Query2Result> executeJob() throws ExecutionException, InterruptedException {
        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<Integer,Pair<String,String>> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<Integer, Pair<String,String>> job = tracker.newJob(source);

        Map<Pair<String,String>,Integer> aux = job
                .mapper(new Query2FirstMapper())
                .reducer(new Query2FirstReducer())
                .submit()
                .get();

        auxMap.putAll(aux);
        final KeyValueSource<Pair<String,String>,Integer> secondSource = KeyValueSource.fromMap(auxMap);
        final Job<Pair<String,String>,Integer>  jobSecond = tracker.newJob(secondSource);

        return jobSecond
                .mapper(new Query2SecondMapper())
                .reducer(new Query2SecondReducer())
                .submit(new Query2Collator())
                .get();
    }

    @Override
    public void close() {
        Optional.ofNullable(infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(ticketsMap).ifPresent(MultiMap::clear);
        Optional.ofNullable(auxMap).ifPresent(Map::clear);
        super.close();
    }

    public static void main(String[] args) {

        try(Query2Client client = new Query2Client("query2")){

            //Load data
            client.loadInfractions();
            client.loadTickets();

            System.out.println("Started");
            //Execute job
            SortedSet<Query2Result> ans = client.executeJob();
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