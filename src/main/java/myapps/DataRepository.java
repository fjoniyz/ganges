package myapps;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

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
     * @param value value to be saved
     */
    public void saveValue(String value) {
        String key = Integer.toString(ThreadLocalRandom.current().nextInt(0, 1000 + 1));
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
        List<String> values = new ArrayList<>();
        for(String key: keys){
            values.add(connection.get(key));
        }
        return values;
    }

    public List<Map<String, Double>> getValuesByKeys(String[] entryKeys) {
        List<Map<String, Double>> entries = new ArrayList<>();
        for (String redisKey : connection.keys("*")) {
            HashMap<String, Double> entry = new HashMap<>();
            for (String entryKey : entryKeys) {
                entry.put(entryKey, Double.parseDouble(connection.hget(redisKey, entryKey)));
            }
            entries.add(entry);
        }
        return entries;
    }

    public void deleteDataByKey(String key) {
        connection.del(key);
    }

    //TODO: Make sure tihs doesn't delete all entries with the same key
    public void deleteEntryByKey(int index, String key) {
        List<String> keys = connection.keys(key);
        if (!keys.isEmpty()) {
            String indexKey = keys.get(index);
            connection.del(key,indexKey);
        }
    }
}
