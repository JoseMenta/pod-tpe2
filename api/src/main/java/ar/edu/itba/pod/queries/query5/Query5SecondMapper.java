package ar.edu.itba.pod.queries.query5;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query5SecondMapper implements Mapper<String, Integer, Integer, String> {
    @Override
    public void map(String s, Integer prom, Context<Integer, String> context) {
        if (prom >= 100) {
            context.emit((int) ((prom / 100) * 100), s);
        }
    }
}
