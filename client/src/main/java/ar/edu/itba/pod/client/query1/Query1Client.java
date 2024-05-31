package ar.edu.itba.pod.client.query1;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import ar.edu.itba.pod.data.results.Query1Result;
import ar.edu.itba.pod.queries.query1.Query1Collator;
import ar.edu.itba.pod.queries.query1.Query1Combiner;
import ar.edu.itba.pod.queries.query1.Query1Mapper;
import ar.edu.itba.pod.queries.query1.Query1Reducer;
import com.hazelcast.core.MultiMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import static ar.edu.itba.pod.Util.QUERY_1_NAMESPACE;

public class Query1Client extends QueryClient {



    //TODO: ver si no es mejor guardar directo <String, String>, lo hago asi por ahora (debería cambiarse el keyMapper)
    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;

    public Query1Client(String query){
        super(query);
        this.infractionsMap = hazelcast.getMap(QUERY_1_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(QUERY_1_NAMESPACE);
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

    @Override
    public void close() {
        Optional.ofNullable(infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(ticketsMap).ifPresent(MultiMap::clear);
        super.close();
    }

    public SortedSet<Query1Result> executeJob() throws ExecutionException, InterruptedException {
        final JobTracker tracker = this.hazelcast.getJobTracker(Util.HAZELCAST_NAMESPACE);
        final KeyValueSource<String,Ticket> source = KeyValueSource.fromMultiMap(ticketsMap);
        final Job<String, Ticket> job = tracker.newJob(source);

        return job
                .mapper(new Query1Mapper())
                .combiner(new Query1Combiner())
                .reducer(new Query1Reducer())
                .submit(new Query1Collator())
                .get();
    }

    public static void main(String[] args) {


        try(Query1Client client = new Query1Client("query1")){
            //Load data
            client.loadInfractions();
            client.loadTickets();

            //Execute job
            SortedSet<Query1Result> ans = client.executeJob();

            //Print results
            ans.forEach(System.out::println);

        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }




}
