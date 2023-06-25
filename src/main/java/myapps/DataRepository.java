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

    public DataRepository() {
        redisClient = new RedisClient(RedisURI.create("redis://localhost/"));
        connection = redisClient.connect();
    }

    /**
     * Save value in cache
     * @param value value to be saved
     */
    public void saveValue(String value) {
        String key = Integer.toString(ThreadLocalRandom.current().nextInt(0, 1000 + 1));
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

//    /**
//     * Retrieve all keys of Redis entries using external script getKeys.sh
//     * @return keys of Redis entries
//     */
//    private static List<String> getKeys(){
//        List<String> result = new ArrayList<>();
//        String userDirectory = System.getProperty("user.dir");
//        System.out.printf("Dir: " + userDirectory);
//        try {
//            Runtime r = Runtime.getRuntime();
//
//            Process p = r.exec(userDirectory + "/getKeys.sh");
//
//            BufferedReader in =
//                    new BufferedReader(new InputStreamReader(p.getInputStream()));
//            String inputLine;
//            while ((inputLine = in.readLine()) != null) {
//                System.out.println(inputLine);
//                result.add(inputLine);
//            }
//            in.close();
//
//        } catch (IOException e) {
//            System.out.println(e);
//        }
//        return result;
//    }
}
