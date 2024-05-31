package ar.edu.itba.pod.client.query1;

import ar.edu.itba.pod.client.QueryClient;
import com.hazelcast.core.MultiMap;

public class Query1Client extends QueryClient {
    

    @Override
    public void close() {
        super.close();
    }

    public static void main(String[] args) {

        try(Query1Client client = new Query1Client()){
        }



    }



}
