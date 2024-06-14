package ar.edu.itba.pod.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

public class HazelcastTest {
    private TestHazelcastFactory hazelcastFactory;
    private HazelcastInstance member, client;
    @Before
    public void setUp() {
        hazelcastFactory = new TestHazelcastFactory();
        // Group Config
        GroupConfig groupConfig = new
                GroupConfig().setName("gX").setPassword("gX-pass");
        // Config
        Config config = new Config().setGroupConfig(groupConfig);
        MultiMapConfig multiMapConfig = new MultiMapConfig();
        multiMapConfig.setName("default");
        multiMapConfig.setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        config.addMultiMapConfig(multiMapConfig);
        member = hazelcastFactory.newHazelcastInstance(config);
        // Client Config
        ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }
    // Test add multiple time the same (key,value) and check if the value is added multiple times
    @Test
    public void simpleListTest() {
        String mapName = "testMap";
        MultiMap<Integer, String> testMapFromMember = member.getMultiMap(mapName);
        testMapFromMember.put(1, "test1");
        testMapFromMember.put(1, "test1");
        MultiMap<Integer, String> testMap = client.getMultiMap(mapName);
        Collection<String> value = testMap.get(1);
        Assert.assertEquals(2, value.size());
    }
    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }
}