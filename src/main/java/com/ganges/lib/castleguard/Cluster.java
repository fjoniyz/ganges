package com.ganges.lib.castleguard;

import org.apache.commons.lang3.Range;

import java.util.*;
import java.lang.String;
import java.lang.Float;

public class Cluster {
    List<Item> contents;
    Map<String, Range<Integer>> ranges;
    // TODO: change type Object
    Set<Object> diversity;
    Map<String, Float> sample_values;

    public Cluster(List<String> headers){
        // Initialises the cluster
        this.contents= new ArrayList<>();
        this.ranges = new HashMap<>();
        // Ranges method -> in Python zero arguments and initialized with zeros
        headers.forEach(header -> {
            this.ranges.put(header, Range.between(0, 0));
        });
        this.diversity = new HashSet();
        this.sample_values = new HashMap<>();
    }
    void insert(Item element){
        // Inserts a tuple into the cluster
        // Args:
        //            element (Item): The element to insert into the cluster
        this.contents.add(element);

        // Check whether the item is already in a cluster
        if (element.parent != null){
            // If it is, remove it so that we do not reach an invalid state
            // TODO: How to remove element here (conflict with datatypes)
            element.parent.remove(element);
        }
        // Add sensitive attribute value to the diversity of cluster
        this.diversity.add(element.sensitive_attr);
        element.parent = this;
    }

    void remove(Item element){
       //  Removes a tuple from the cluster
       //  Args:
        //            element: The element to remove from the cluster
        this.contents.remove(element);
        for(Item e : this.contents){
            if (! (Objects.equals(e.sensitive_attr, element.sensitive_attr))) {
                this.diversity.remove(element.sensitive_attr);
            }

        }
    }
    // TODO no void functions -> returns pd.series in python
     void generalise(){

        }

     float tuple_enlargement(Item item, HashMap<String, Range<Integer>> global_ranges){
            /*Calculates the enlargement value for adding <item> into this cluster

        Args:
            item: The tuple to calculate enlargement based on
            global_ranges: The globally known ranges for each attribute

    Returns: The information loss if we added item into this cluster
           */
        Float given = this.information_loss_given_t(item, global_ranges);
        Float current = this.information_loss(global_ranges);
        return (given - current) /this.ranges.size();
    }

    float cluster_enlargement(Cluster cluster, HashMap<String, Range<Integer>> global_ranges){
        /*Calculates the enlargement value for merging <cluster> into this cluster

        Args:
        cluster: The cluster to calculate information loss for
        global_ranges: The globally known ranges for each attribute

        Returns: The information loss upon merging cluster with this cluster*/
        Float given = this.information_loss_given_c(cluster, global_ranges);
        Float current = this.information_loss(global_ranges);
        return (given - current) / this.ranges.size();
    }

    float information_loss_given_t(Item item, HashMap<String, Range<Integer>> global_ranges){
        /*Calculates the information loss upon adding <item> into this cluster

        Args:
        item: The tuple to calculate information loss based on
        global_ranges: The globally known ranges for each attribute

        Returns: The information loss given that we insert item into this cluster*/
        float loss = 0F;

        // For each range, check if <item> would extend it
        Range <Integer> updated = null;
        for (Map.Entry<String, Range<Integer>> header: this.ranges.entrySet()){
            Range <Integer> global_range = global_ranges.get(header.getKey());
            updated = Range.between((int) Math.min(header.getValue().getMinimum(), item.data.get(header.getKey())), (int) Math.max(header.getValue().getMaximum(), item.data.get(header.getKey())));
            loss += range_information_loss(updated, global_range);
        }
        return loss;
    }
    // TODO: this is actually a Range Method
    float range_information_loss(Range<Integer> actual, Range<Integer> other){
        float diff_self = Math.abs(actual.getMaximum()-actual.getMinimum());
        float diff_other = Math.abs(other.getMaximum()-other.getMinimum());;
        if (diff_other == 0){
            return 0F;
        }
        return diff_self/diff_other;
    }

    float information_loss_given_c(Cluster cluster, HashMap<String, Range<Integer>> global_ranges) {
       /* Calculates the information loss upon merging <cluster> into this cluster

        Args:
            cluster: The cluster to calculate information loss based on
            global_ranges: The globally known ranges for each attribute

        Returns: The information loss given that we merge cluster with this cluster*/
        float loss = 0F;

        // For each range, check if <item> would extend it
        Range <Integer> updated = null;
        for (Map.Entry<String, Range<Integer>> header: this.ranges.entrySet()){
            Range <Integer> global_range = global_ranges.get(header.getKey());
            updated = Range.between((int) Math.min(header.getValue().getMinimum(), cluster.ranges.get(header.getKey()).getMinimum()), (int) Math.max(header.getValue().getMaximum(),cluster.ranges.get(header.getKey()).getMinimum()));
            loss += range_information_loss(updated, global_range);
        }
        return loss;
    }

    float information_loss(HashMap<String, Range<Integer>> global_ranges){
        /*Calculates the information loss of this cluster

        Args:
        global_ranges: The globally known ranges for each attribute

        Returns: The current information loss of the cluster*/
        float loss = 0F;

        // For each range, check if <item> would extend it
        Range <Integer> updated = null;
        for (Map.Entry<String, Range<Integer>> header: this.ranges.entrySet()){
            Range <Integer> global_range = global_ranges.get(header.getKey());
            loss += range_information_loss(header.getValue(), global_range);
        }
        return loss;
    }

    // TODO: find Datatype of other
    float distance(Range<Integer> other){
        /*Calculates the distance from this tuple to another

        Args:
        other: The tuple to calculate the distance to

        Returns: The distance to the other tuple*/
        return 1.0F;
    }

    boolean within_bounds(Item item){
       /* Checks whether a tuple is within all the ranges of the this
        cluster, eg. would cause no information loss on being entered.

                Args:
        item: The tuple to perform bounds checking on

        Returns: Whether or not the tuple is within the bounds of the cluster*/
        return true;
    }


}