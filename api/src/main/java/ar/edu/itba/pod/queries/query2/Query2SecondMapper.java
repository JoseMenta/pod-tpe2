package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query2SecondMapper implements Mapper<Pair<String, String>,Integer,String,Pair<String,Integer>> {
    @Override
    public void map(Pair<String, String> key, Integer value, Context<String, Pair<String, Integer>> context) {
        context.emit(key.first(),new Pair<>(key.second(),value));
    }
}
