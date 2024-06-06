package ar.edu.itba.pod.queries.query1;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query1Mapper implements Mapper<Integer, String, String, Integer> {

    @Override
    public void map(Integer key, String val, Context<String, Integer> context) {
        context.emit(val,1);
    }
}
