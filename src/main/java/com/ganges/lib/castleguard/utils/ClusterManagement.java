package com.ganges.lib.castleguard.utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.Range;

import com.ganges.lib.castleguard.Cluster;
import com.ganges.lib.castleguard.Item;

public class ClusterManagement {
    
    private int k;
    private int l;
    private int mu;
    private List<Cluster> bigGamma = new ArrayList<>(); // Set of non-ks anonymised clusters
    private List<Cluster> bigOmega = new ArrayList<>(); // Set of ks anonymised clusters
    private List<String> headers;
    private String sensitiveAttribute;
    private double tau;
    private List<Float> recentLosses = new ArrayList<>();


    public ClusterManagement(int k, int l, int mu, List<String> headers, String sensitiveAttribute) {
        this.k = k;
        this.l = l;
        this.mu = mu;
        this.headers = headers;
        this.sensitiveAttribute = sensitiveAttribute;
    }

    public List<Cluster> getBigGamma() {
        return bigGamma;
    }

    public List<Cluster> getBigOmega() {
        return bigOmega;
    }

    public void addToBigGamma(Cluster c) {
        this.bigGamma.add(c);
    }

    public void addToBigOmega(Cluster c) {
        this.bigOmega.add(c);
    }

    public void removeFromBigGamma(Cluster c) {
        this.bigGamma.remove(c);
    }

    public void removeFromBigOmega(Cluster c) {
        this.bigOmega.remove(c);
    }

    /**
     * Group every tuple by the sensitive attribute
     * @param c
     * @param sensitiveAttribute
     * @return
     */
    public List<Cluster> splitL(Cluster c, List<String> headers,  HashMap<String, Range<Float>> globalRanges) {
        List<Cluster> sc = new ArrayList<>();

        // Group every tuple by the sensitive attribute
        Map<Object, List<Item>> buckets = generateBuckets(c, this.sensitiveAttribute);

        // if number of buckets is less than l, cannot split
        if (buckets.size() < l) {
            sc.add(c);
            return sc;
        }

        // calculate the bucket size
        int count = 0;
        for (List<Item> bucket : buckets.values()) {
            count += bucket.size();
        }

        // While length of buckets greater than l and more than k tuples
        while (buckets.size() >= l && count >= k) {
            // Pick a random tuple from a random bucket
            List<Object> bucketPids = new ArrayList<>(buckets.keySet());
            Random random = new Random();
            Object pid = bucketPids.get(random.nextInt(bucketPids.size()));
            List<Item> bucket = buckets.get(pid);
            Item t = bucket.remove(random.nextInt(bucket.size()));

            // Create a new subcluster over t
            Cluster cnew = new Cluster(headers);
            cnew.insert(t);

            // Check whether the bucket is empty
            if (bucket.isEmpty()) {
                buckets.remove(pid);
            }

            List<Object> emptyBuckets = new ArrayList<>();

            for (Map.Entry<Object, List<Item>> entry : buckets.entrySet()) {
                Object currentPid = entry.getKey();
                List<Item> currentBucket = entry.getValue();

                // Sort the bucket by the enlargement value of that cluster
                currentBucket.sort(Comparator.comparingDouble(tuple -> c.tupleEnlargement(tuple, globalRanges)));

                // Count the number of tuples we have
                int totalTuples = 0;
                for (List<Item> b : buckets.values()) {
                    totalTuples += b.size();
                }
                // Calculate the number of tuples we should take
                int chosenCount = (int) Math.max(k * (currentBucket.size() / (double) totalTuples), 1);
                // Get the subset of tuples
                List<Item> subset = currentBucket.subList(0, chosenCount);

                // Insert the top Tj tuples in a new cluster
                for (Item item : subset) {
                    cnew.insert(item);
                    currentBucket.remove(item);
                }

                // if bucket is empty delete the bucket
                if (currentBucket.isEmpty()) {
                    emptyBuckets.add(currentPid);
                }
            }

            for (Object pidToRemove : emptyBuckets) {
                buckets.remove(pidToRemove);
            }

            sc.add(cnew);
        }

        // For all remaining tuples in this cluster, add them to the nearest cluster
        for (List<Item> bucket : buckets.values()) {
            for (Item t : bucket) {
                Cluster cluster = Collections.min(sc, Comparator.comparingDouble(cluster1 -> cluster1.distance(t)));
                cluster.insert(t);
            }

            bucket.clear();
        }

        // Add tuples to the original cluster
        for (Cluster cluster : sc) {
            for (Item tuple : cluster.getContents()) {
                List<Item> g = new ArrayList<>();
                for (Item t : c.getContents()) {
                    if (Objects.equals(t.getData().get("pid"), tuple.getData().get("pid"))) {
                        g.add(t);
                    }
                }
                for (Item t : g) {
                    cluster.insert(t);
                }
            }
            this.bigGamma.add(cluster);
        }

        return sc;
    }

    private Map<Object, List<Item>> generateBuckets(Cluster cluster, String sensitiveAttribute) {
        Map<Object, List<Item>> buckets = new HashMap<>();

        for (Item t : cluster.getContents()) {
            // Get the value for the sensitive attribute for this tuple
            Object sensitiveValue = t.getData().get(this.sensitiveAttribute);

            // If it isn't in our map, make an empty list for it
            if (!buckets.containsKey(sensitiveValue)) {
                buckets.put(sensitiveValue, new ArrayList<>());
            }

            // Insert the tuple into the cluster
            buckets.get(sensitiveValue).add(t);
        }

        return buckets;
    }


    public Cluster mergeClusters(Cluster c, HashMap<String, Range<Float>> globalRanges) {
        List<Cluster> gamma_c = new ArrayList<>(this.bigGamma);
        gamma_c.remove(c);

        while (c.getContents().size() < this.k || c.getDiversity().size() < this.l) {
            // Get the cluster with the lowest enlargement value
            Cluster lowestEnlargementCluster = Collections.min(gamma_c, Comparator.comparingDouble(cl -> cl.clusterEnlargement(cl, globalRanges)));
            List<Item> items = new ArrayList<>(lowestEnlargementCluster.getContents());

            for (Item t : items) {
                c.insert(t);
            }

            this.bigGamma.remove(lowestEnlargementCluster);
            gamma_c.remove(lowestEnlargementCluster);
        }

        return c;
    }


    public void updateTau(HashMap<String, Range<Float>> globalRanges) {
        this.tau = Double.POSITIVE_INFINITY;
        if (!recentLosses.isEmpty()) {
            tau = recentLosses.stream().reduce(0F, Float::sum);
        } else if (!bigGamma.isEmpty()) {
            int sampleSize = Math.min(bigGamma.size(), 5);
            List<Cluster> chosen = Utils.randomChoice(bigGamma, sampleSize);

            float totalLoss = chosen.stream().map(c -> c.informationLoss(globalRanges)).reduce(0F, Float::sum);
            tau = totalLoss / sampleSize;
        }
    }

    public void updateLoss(Cluster c, HashMap<String, Range<Float>> globalRanges) {
        float loss = c.informationLoss(globalRanges);
        recentLosses.add(loss);
        if (recentLosses.size() > this.mu) {
            recentLosses.remove(0);
        }
        this.updateTau(globalRanges);
    }
}
