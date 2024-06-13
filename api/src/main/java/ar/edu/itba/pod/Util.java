package ar.edu.itba.pod;

public class Util {

    //TODO: use config file
    public static final String HAZELCAST_GROUP_NAME = "g6";
    public static final String HAZELCAST_GROUP_PASSWORD = "pod";
    public static final String HAZELCAST_NAMESPACE = "g6-collections";
    // https://docs.hazelcast.org/docs/3.8.6/manual/html-single/index.html#interfaces
    public static final String HAZELCAST_DEFAULT_MASK = "127.0.0.*";

    public static final String QUERY_1_NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q1";
    public static final String QUERY_2_NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q2";
    public static final String QUERY_3_NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q3";
    public static final String QUERY_4_NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q4";
    public static final String QUERY_5_NAMESPACE = Util.HAZELCAST_NAMESPACE + "-q5";
}
