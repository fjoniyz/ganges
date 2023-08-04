package myapps;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Repository for cached non-anonymized data that is used to provide state for anonymization algorithms
 * Currently using Redis as cache
 */
public class DataRepository {
    RedisClient redisClient;
    RedisConnection<String, String> connection;

    public DataRepository() {
        redisClient = new RedisClient(RedisURI.create("redis://localhost/"));
        connection = redisClient.connect();
    }

    public void open() {
        if (connection.isOpen()){
            connection.close();
        }
        connection = redisClient.connect();
    }

    public void close() {
        if (connection.isOpen()){
            connection.close();
        }
    }

    /**
     * Save value in cache
     * @param values
     */
    public void saveValues(HashMap<String, Double> values) {
        int lastKey = getHighestKey();
        String key = Integer.toString(lastKey + 1);
        for (Map.Entry<String, Double> value : values.entrySet()) {
            connection.hset(key, value.getKey(), value.getValue().toString());
        }
    }

    public List<AnonymizationItem> getValuesByKeys(String[] valueKeys) {
        List<String> keys = connection.keys("*");
        List<Integer> keysAsInts = new ArrayList<>();
        for(String key: keys){
            keysAsInts.add(Integer.parseInt(key));
        }
        Collections.sort(keysAsInts);
        List<AnonymizationItem> values = new ArrayList<>();
        for (Integer redisKey : keysAsInts) {
            HashMap<String, Double> entry = new HashMap<>();
            String id = redisKey.toString();
            AnonymizationItem anonymizationItem = new AnonymizationItem(id, entry);
            for(int i = 0; i < valueKeys.length; i++) {
                entry.put(valueKeys[i], Double.parseDouble(connection.hget(Integer.toString(redisKey),
                        valueKeys[i])));
            }
            values.add(anonymizationItem);
        }
        return values;
    }

    public int getHighestKey() {
        List<String> keys = connection.keys("*");
        int maxKey = 0;
        for(String s : keys){
            int key = Integer.parseInt(s);
            if(key > maxKey){
                maxKey = key;
            }
        }
        return maxKey;
    }
}
