package myapps;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Repository for cached non-anonymized data that is used to provide state for anonymization algorithms
 * Currently using Redis as cache
 */
public class DataRepository {
    RedisClient redisClient;
    RedisConnection<String, String> connection;
    int lastKey = 0;

    public DataRepository() {
        redisClient = new RedisClient(RedisURI.create("redis://localhost/"));
        connection = redisClient.connect();
    }

    /**
     * Save value in cache
     * @param value value to be saved
     */
    public void saveValue(String value) {
        String key = Integer.toString(lastKey + 1);
        lastKey = Integer.parseInt(key);
        connection.set(key, value);
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
}
