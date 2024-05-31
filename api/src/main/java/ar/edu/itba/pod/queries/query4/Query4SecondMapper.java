package ar.edu.itba.pod.queries.query4;

import ar.edu.itba.pod.data.Pair;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query4SecondMapper implements Mapper<Pair<String, String>, Integer, String, Pair<String, Integer>> {

    /**
     * Given the (<neighbourhood, plate>, total) pair, emits the (neighbourhood, <plate, total>) pair
     * It just relocate the pair data within its key and value
     */
    @Override
    public void map(Pair<String, String> pairIn, Integer total, Context<String, Pair<String, Integer>> context) {
        final String neighbourhood = pairIn.getFirst();
        final String plate = pairIn.getSecond();
        context.emit(neighbourhood, new Pair<>(plate, total));
    }
}
