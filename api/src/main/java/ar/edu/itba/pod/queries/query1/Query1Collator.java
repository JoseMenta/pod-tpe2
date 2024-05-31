package ar.edu.itba.pod.queries.query1;

//TODO: revisar si puede ir sólo en el cliente

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.results.Query1Result;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class Query1Collator implements Collator<Map.Entry<String,Integer>, SortedSet<Query1Result>>, HazelcastInstanceAware {

    Map<String, Infraction> infractions;

    private static final Comparator<Query1Result> COMPARATOR = Comparator
            .comparing(Query1Result::tickets)
            .reversed()
            .thenComparing(Query1Result::infraction);

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        hazelcastInstance.getMap(Util.QUERY_1_NAMESPACE);
    }

    @Override
    public SortedSet<Query1Result> collate(Iterable<Map.Entry<String, Integer>> values) {
        SortedSet<Query1Result> results = new TreeSet<>(COMPARATOR);
        for(Map.Entry<String, Integer> entry : values) {
            results.add(new Query1Result(infractions.get(entry.getKey()).description(), entry.getValue()));
        }
        return results;
    }
}
