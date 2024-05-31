package ar.edu.itba.pod.client.query1;

import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.utilities.City;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Ticket;
import com.hazelcast.core.MultiMap;

import java.util.Map;
import java.util.Optional;

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
                Infraction::code,
                i->i,
                infractionsMap::put);
    }

    private void loadTickets(){

        switch (this.city){
            case NYC:
                loadData(this.ticketPath,
                        this::nyTicketMapper, //TODO: change based on NY or Chicago
                        Ticket::infractionCode,
                        i -> i,
                        ticketsMap::put);
                break;
            case CHI:
                loadData(this.ticketPath,
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
        Optional.ofNullable(infractionsMap).ifPresent(Map::clear);
        Optional.ofNullable(ticketsMap).ifPresent(MultiMap::clear);
        super.close();
    }

    public static void main(String[] args) {


        try(Query1Client client = new Query1Client("query1")){

            //Load data
            client.loadInfractions();
            client.loadTickets();
        }



        }




}
