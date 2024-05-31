package ar.edu.itba.pod.client.query5;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.util.Map;

public class Query5Client extends QueryClient {

    private static final String NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q5";
    private final Map<String, Infraction> infractionsMap;

    private final MultiMap<String, Ticket> ticketsMap;


    public Query5Client(String query) {
        super(query);
        this.infractionsMap = hazelcast.getMap(NAMESPACE);
        this.ticketsMap = hazelcast.getMultiMap(NAMESPACE);

    }
    private void loadInfractions(){
        loadData(this.infractionPath,
                this::infractionMapper,
                Infraction::code,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets( ){

        switch (this.city){
            case NYC:
                loadData(this.csvPath,
                        this::nyTicketMapper, //TODO: change based on NY or Chicago
                        Ticket::infractionCode,
                        i -> i,
                        ticketsMap::put);
                break;
            case CHI:
                loadData(this.csvPath,
                        this:: chicagoTicketMapper, //TODO: change based on NY or Chicago
                        Ticket::infractionCode,
                        i -> i,
                        ticketsMap::put);
                break;
            default:
                throw new IllegalArgumentException("Invalid city");
        }

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
