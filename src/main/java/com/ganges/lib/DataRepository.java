package com.ganges.lib;

import com.ganges.lib.AnonymizationItem;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

import java.util.Collections;
import java.util.ArrayList;
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
     * @param values
     */
    public void saveValues(Map<String, String> values) {
        int lastKey = getHighestKey();
        String key = Integer.toString(lastKey + 1);
        for (Map.Entry<String, String> value : values.entrySet()) {
            connection.hset(key, value.getKey(), value.getValue());
        }
    }

    public List<AnonymizationItem> getValuesByKeys(List<String> valueKeys) {
        List<String> keys = connection.keys("*");
        List<Integer> keysAsInts = new ArrayList<>();
        for(String key: keys){
            keysAsInts.add(Integer.parseInt(key));
        }
        Collections.sort(keysAsInts);
        List<AnonymizationItem> values = new ArrayList<>();
        for (Integer redisKey : keysAsInts) {
            String id = redisKey.toString();

            Map<String, String> entryValues = connection.hgetall(Integer.toString(redisKey));
            HashMap<String, Double> anonymizedValues = new HashMap<>();
            HashMap<String, String> nonAnonymizedValues = new HashMap<>();

            for (Map.Entry<String, String> value : entryValues.entrySet()) {
                if (valueKeys.contains(value.getKey())) {
                    anonymizedValues.put(value.getKey(), Double.parseDouble(value.getValue()));
                } else {
                    nonAnonymizedValues.put(value.getKey(), value.getValue());
                }
            }

            AnonymizationItem anonymizationItem = new AnonymizationItem(id, anonymizedValues, nonAnonymizedValues);
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
