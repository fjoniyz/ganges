package myapps;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * @param value value to be saved
     */
    public void saveValue(String value) {
        int lastKey = getHighestKey();
        String key = Integer.toString(lastKey + 1);
        connection.set(key, value);
    }

    public void saveValues(String key, HashMap<String, Double> values) {
        for (Map.Entry<String, Double> value : values.entrySet()) {
            connection.hset(key, value.getKey(), value.getValue().toString());
        }
    }

    /**
     * Get all saved values from cache
     * @return array of saved values
     */
    public List<String> getValues(){
        List<String> keys = connection.keys("*");
        List<Integer> keysAsInts = new ArrayList<>();
        for(String key: keys){
            keysAsInts.add(Integer.parseInt(key));
        }
        Collections.sort(keysAsInts);
        List<String> values = new ArrayList<>();
        for(Integer key: keysAsInts){
            values.add(connection.get(Integer.toString(key)));
        }
        return values;
    }

    public int getHighestKey() {
        List<String> keys = connection.keys("*");
        int maxKey = 0;
        for (String s : keys) {
            int key = Integer.parseInt(s);
            if (key > maxKey) {
                maxKey = key;
            }
        }
        return maxKey;
    }
    public List<Map<String, Double>> getValuesByKeys(String[] entryKeys) {
        List<Map<String, Double>> entries = new ArrayList<>();
        for (String redisKey : connection.keys("*")) {
            HashMap<String, Double> entry = new HashMap<>();
            for(int i = 0; i < entryKeys.length; i++) {
                entry.put(entryKeys[i], Double.parseDouble(connection.hget(redisKey,
                    entryKeys[i])));
            }
            entries.add(entry);
        }
        return entries;
    }
}
