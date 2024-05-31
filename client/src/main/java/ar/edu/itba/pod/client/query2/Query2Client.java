package ar.edu.itba.pod.client.query2;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.util.Map;

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