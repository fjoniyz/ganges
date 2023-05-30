package com.ganges.lib.castleguard;

import org.apache.commons.lang3.Range;

import java.util.*;

public class CastleGuard {
    private final CGConfig config;
    private List<String> headers;
    private Float sensitiveAttr;
    private Deque<Item> items;
    private List<Cluster> bigGamma;
    private HashMap<String, Range<Float>> globalRanges;
    double tau;

    public CastleGuard(CGConfig config, List<String> headers, Float sensitiveAttr) {
        this.config = config;
        this.headers = headers;
        this.sensitiveAttr = sensitiveAttr;
        this.bigGamma = new ArrayList<>();
        this.globalRanges = new HashMap<>();
        this.tau = Double.POSITIVE_INFINITY;
    }

    /**
     * insert() in castle.py
     * @param data value tuple
     */
    public void insertData(HashMap<String, Float> data) {
        Random rand = new Random();
        if (config.isUseDiffPrivacy() && rand.nextDouble(1) > config.getBigBeta()) {
            return;
        }
        Item item = new Item(data, this.headers, this.sensitiveAttr);
        updateGlobalRanges(item);

        if (config.isUseDiffPrivacy()) {
            item = perturb(item);
        }
        Optional<Cluster> cluster = bestSelection(item);
        if (!cluster.isPresent()) {
            // TODO: create cluster here
        } else {
            insertIntoCluster(item, cluster.get());
        }

        items.add(item);
        if (items.size() > config.getDelta()) {
            delayConstraint(items.pop());
        }
    }

    /**
     * Called by the object that manages anonymization class
     * @param item
     * @return
     */
    public Object outputCluster(Item item) {
        return null;
    }

    private void insertIntoCluster(Item item, Object cluster) {
        // TODO: Implement (basis)

    }

    private void createCluster() {
        // TODO: Implement
    }

    private void delayConstraint(Item item) {
        // TODO: Implement
    }

    /**
     * fudge_tuple() in castle.py
     * @return
     */
    private Item perturb(Item item) {
        // TODO: Implement
        return null;
    }

    /**
     * best_selection() in castle.py
     * @return cluster or null
     */
    private Optional<Cluster> bestSelection(Item item) {

        // Need to be tested

        Set<Float> e = new HashSet<>();

        for (Cluster cluster : bigGamma) {
            e.add(cluster.tuple_enlargement(item, globalRanges));
        }

        if (e.isEmpty()) {
            return Optional.empty();
        }

        float minima = Collections.min(e);

        List<Cluster> setCmin = new ArrayList<>();

        for (Cluster cluster : bigGamma) {
            if (cluster.tuple_enlargement(item, globalRanges) == minima) {
                setCmin.add(cluster);
            }
        }

        Set<Cluster> setCok = new HashSet<>();

        for (Cluster cluster : setCmin) {
            double ilcj = cluster.information_loss_given_t(item, globalRanges);
            if (ilcj <= tau) {
                setCok.add(cluster);
            }
        }

        if (setCok.isEmpty()) {
            if (this.config.getBeta() <= bigGamma.size()) {
                Random rand = new Random();
                int randomIndex = rand.nextInt(setCmin.size());
                List<Cluster> setCminList = new ArrayList<>(setCmin);
                return Optional.of(setCminList.get(randomIndex));
            }

            return Optional.empty();
        }

        List<Cluster> setCokList = new ArrayList<>(setCok);
        Random rand = new Random();
        int randomIndex = rand.nextInt(setCokList.size());
        return Optional.of(setCokList.get(randomIndex));
    }

    void updateGlobalRanges(Object data) {
        // TODO: Implement (Basis)
    }

    public List<Cluster> split(Cluster c) {
        List<Cluster> sc = new ArrayList<>();

        HashMap<Float, List<Item>> buckets = new HashMap<>();

        for (Item t : c.getContents()) {

            // not sure about the Float -> Should be int??
            // also do we get the pid like that?
            Float pid = t.getData().get("pid");
            if (!buckets.containsKey(pid)) {
                buckets.put(pid, new ArrayList<>());
            }
            buckets.get(pid).add(t);
        }

        while (this.config.getK() <= buckets.size()) {
            Random rand = new Random();
            List<Float> pidList = new ArrayList<>(buckets.keySet());
            Float randomPid = pidList.get(rand.nextInt(pidList.size()));
            List<Item> bucket = buckets.get(randomPid);
            Item t = bucket.remove(rand.nextInt(bucket.size()));

            Cluster cnew = new Cluster(this.headers);
            cnew.insert(t);

            if (bucket.isEmpty()) {
                buckets.remove(randomPid);
            }

            List<Item> heap = new ArrayList<>();

            for (Map.Entry<Float, List<Item>> entry : buckets.entrySet()) {
                Float key = entry.getKey();
                List<Item> value = entry.getValue();
                if (Objects.equals(key, randomPid)) {
                    continue;
                }
                Item randomTuple = value.get(rand.nextInt(value.size()));
                heap.add(randomTuple);
            }

            heap.sort(Comparator.comparingDouble(t::tupleDistance));

            for (Item node : heap) {
                cnew.insert(node);
                List<Float> containing = new ArrayList<>();

                for (Float key : buckets.keySet()) {
                    if (buckets.get(key).contains(node)) {
                        containing.add(key);
                    }
                }

                for (Float key : containing) {
                    buckets.get(key).remove(node);
                    if (buckets.get(key).isEmpty()) {
                        buckets.remove(key);
                    }
                }
            }

            sc.add(cnew);
        }

        for (List<Item> bi : buckets.values()) {
            Random rand = new Random();
            Item ti = bi.get(rand.nextInt(bi.size()));
            Cluster nearest = Collections.min(sc, Comparator.comparingDouble(cluster -> cluster.tuple_enlargement(ti, this.globalRanges)));
            for (Item t : bi) {
                nearest.insert(t);
            }
        }

        return sc;
    }

    public List<Cluster> splitL(Cluster c) {
        List<Cluster> sc = new ArrayList<>();

        // Group every tuple by the sensitive attribute
        Map<Object, List<Item>> buckets = generateBuckets(c);

        // if number of buckets is less than l, cannot split
        if (buckets.size() < this.config.getL()) {
            sc.add(c);
            return sc;
        }



        // calculate the bucket size
        int count = 0;
        for (List<Item> bucket : buckets.values()) {
            count += bucket.size();
        }

        // While length of buckets greater than l and more than k tuples
        while (buckets.size() >= this.config.getL() && count >= this.config.getK()) {
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
                currentBucket.sort(Comparator.comparingDouble(tuple -> c.tuple_enlargement(tuple, globalRanges)));

                // Count the number of tuples we have
                int totalTuples = 0;
                for (List<Item> b : buckets.values()) {
                    totalTuples += b.size();
                }
                // Calculate the number of tuples we should take
                int chosenCount = (int) Math.max(this.config.getK() * (currentBucket.size() / (double) totalTuples), 1);
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
            bigGamma.add(cluster);
        }

        return sc;
    }

    private Map<Object, List<Item>> generateBuckets(Cluster cluster) {
        Map<Object, List<Item>> buckets = new HashMap<>();

        for (Item t : cluster.getContents()) {
            // Get the value for the sensitive attribute for this tuple
            Object sensitiveValue = t.getData().get(this.sensitiveAttr);

            // If it isn't in our map, make an empty list for it
            if (!buckets.containsKey(sensitiveValue)) {
                buckets.put(sensitiveValue, new ArrayList<>());
            }

            // Insert the tuple into the cluster
            buckets.get(sensitiveValue).add(t);
        }

        return buckets;
    }

    public Cluster mergeClusters(Cluster c) {
        List<Cluster> gamma_c = new ArrayList<>(this.bigGamma);
        gamma_c.remove(c);

        while (c.getContents().size() < this.config.getK() || c.diversity.size() < this.config.getL()) {
            // Get the cluster with the lowest enlargement value
            Cluster lowestEnlargementCluster = Collections.min(gamma_c, Comparator.comparingDouble(cl -> cl.cluster_enlargement(cl, this.globalRanges)));
            List<Item> items = new ArrayList<>(lowestEnlargementCluster.getContents());

            for (Item t : items) {
                c.insert(t);
            }

            this.bigGamma.remove(lowestEnlargementCluster);
            gamma_c.remove(lowestEnlargementCluster);
        }

        return c;
    }

}
