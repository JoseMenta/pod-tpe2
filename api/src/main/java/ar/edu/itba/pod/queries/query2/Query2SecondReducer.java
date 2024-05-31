package ar.edu.itba.pod.queries.query2;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Query2SecondReducer implements ReducerFactory<String, Pair<String,Integer>, List<String>>, HazelcastInstanceAware {

    private final Comparator<Pair<String,Integer>> COMPARATOR = Comparator.<Pair<String, Integer>, Integer>comparing(Pair::second).reversed();

    private Map<String, Infraction> infractions;

    private static final int MAX_ELEMENTS = 3;

    @Override
    public Reducer<Pair<String, Integer>, List<String>> newReducer(String s) {
        return new Reducer<Pair<String, Integer>, List<String>>() {

            SortedSet<Pair<String,Integer>> values;

            @Override
            public void beginReduce() {
                this.values = new TreeSet<>(COMPARATOR);
            }

            @Override
            public void reduce(Pair<String, Integer> value) {
                values.add(value);
                if (values.size()>=MAX_ELEMENTS+1){
                    values.removeLast();
                }
            }

            @Override
            public List<String> finalizeReduce() {
                List<String> ans =  values.stream()
                        .map(Pair::first)
                        .limit(MAX_ELEMENTS)
                        .map(infractions::get)
                        .map(Infraction::description)
                        .collect(Collectors.toCollection(ArrayList::new));//use custom collector to have mutable list
                IntStream.range(ans.size(),MAX_ELEMENTS).forEach(i -> ans.add("-"));
                return ans;
            }
        };
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.infractions = hazelcastInstance.getMap(Util.QUERY_2_NAMESPACE);
    }
}
