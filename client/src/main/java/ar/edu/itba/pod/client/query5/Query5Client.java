package ar.edu.itba.pod.client.query5;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.util.Map;

public class Query5Client extends QueryClient {

    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;


    public Query5Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(Util.QUERY_5_NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(Util.QUERY_5_NAMESPACE);

    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::code,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets( ){
        loadData(this.ticketPath,
                getMapper(),
                Ticket::infractionCode,
                i -> i,
                ticketsMap::put);
    }

    @Override
    public void close() {
        super.close();
    }

    public static void main(String[] args) {

        try(Query5Client client = new Query5Client("query5")){

            //Load data
            client.loadInfractions();
            client.loadTickets();
        }


    }
}
