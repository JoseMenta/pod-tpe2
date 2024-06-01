package ar.edu.itba.pod.queries.query5;

import ar.edu.itba.pod.Util;
import ar.edu.itba.pod.data.Infraction;
import ar.edu.itba.pod.data.Pair;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.*;

public class Query5SecondReducer implements ReducerFactory<Integer, String, List<Pair<String, String>>>, HazelcastInstanceAware {

    private transient Map<String, Infraction> infractions;



    @Override
    public Reducer<String, List<Pair<String, String>>> newReducer(Integer i) {
        return new Reducer<String, List<Pair<String, String>>>() {

            private Set<String> infractionCodes;

            @Override
            public void beginReduce() {
                this.infractionCodes = new HashSet<>();
            }

            @Override
            public void reduce(String s) {
                infractionCodes.add(s);
            }

            @Override
            public List<Pair<String, String>> finalizeReduce() {
                List<Pair<String, String>> infractionCodesPair = new ArrayList<>();
                List<String> aux =infractionCodes.stream().map(infractions::get).map(Infraction::getDescription).toList();
                for (int i = 0; i < infractionCodes.size(); i++) {
                    for (int j = i+1; j < infractionCodes.size(); j++) {
                        infractionCodesPair.add(new Pair<>(aux.get(i), aux.get(j)));
                    }
                }
                return infractionCodesPair;
            }
        };
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.infractions = hazelcastInstance.getMap(Util.QUERY_5_NAMESPACE);
    }
}
