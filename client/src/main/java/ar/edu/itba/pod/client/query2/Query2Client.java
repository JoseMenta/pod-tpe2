package ar.edu.itba.pod.client.query2;

import ar.edu.itba.pod.client.QueryClient;
import ar.edu.itba.pod.client.query1.Query1Client;

public class Query2Client extends QueryClient {


    @Override
    public void close() {
        super.close();
    }

    public static void main(String[] args) {

        try(Query1Client client = new Query1Client()){
        }



    }
}