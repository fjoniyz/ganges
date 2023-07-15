package myapps;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    /**
     * Save value in cache
     * @param value value to be saved
     */
    public void saveValue(String value) {
        int lastKey = getHighestKey();
        String key = Integer.toString(lastKey + 1);
        connection.set(key, value);
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
        for(String s : keys){
            int key = Integer.parseInt(s);
            if(key > maxKey){
                maxKey = key;
            }
        }
        return maxKey;
    }
}
