package com.ganges.lib.castleguard;

import org.apache.commons.lang3.Range;
import com.ganges.lib.castleguard.utils.Utils;


import java.util.*;
import java.lang.String;
import java.lang.Float;

public class Cluster {
    List<Item> contents;
    Map<String, Range<Float>> ranges;
    // TODO: change type Object
    Set<Object> diversity;
    Map<String, Float> sample_values;
    Utils utils;

    public Cluster(List<String> headers){
        // Initialises the cluster
        this.contents= new ArrayList<>();
        this.ranges = new HashMap<>();
        // Ranges method -> in Python zero arguments and initialized with zeros
        headers.forEach(header -> {
            this.ranges.put(header, Range.between(0F, 0F));
        });
        this.diversity = new HashSet();
        this.sample_values = new HashMap<>();

        this.utils = new Utils();
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

    // Note: Return value without Item -> In Cluster.py return value (gen_tuple, item)
     Item generalise(Item item){
        Item gen_tuple = item;

         for (Map.Entry<String, Range<Float>> header: this.ranges.entrySet()){
             if (! this.sample_values.containsKey(header.getKey())){
                 // TODO: create Random Array
                 this.sample_values.put(header.getKey()) = np.random.choice(self.contents)[header];
             }
             gen_tuple.data.put("min" + header.getKey(), header.getValue().getMinimum());
             gen_tuple.data.put("spc" + header.getKey(), this.sample_values.get(header.getKey()));
             gen_tuple.data.put("max" + header.getKey(), header.getValue().getMaximum());

             gen_tuple.headers.add("min"+header.getKey());
             gen_tuple.headers.add("spc"+header.getKey());
             gen_tuple.headers.add("max"+header.getKey());

             gen_tuple.headers.remove(header);
             gen_tuple.data.remove(header.getKey());
             gen_tuple.data.remove("pid");
         }
         return gen_tuple;
    }

     float tuple_enlargement(Item item, HashMap<String, Range<Float>> global_ranges){
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

    float cluster_enlargement(Cluster cluster, HashMap<String, Range<Float>> global_ranges){
        /*Calculates the enlargement value for merging <cluster> into this cluster

        Args:
        cluster: The cluster to calculate information loss for
        global_ranges: The globally known ranges for each attribute

        Returns: The information loss upon merging cluster with this cluster*/
        Float given = this.information_loss_given_c(cluster, global_ranges);
        Float current = this.information_loss(global_ranges);
        return (given - current) / this.ranges.size();
    }
    // TODO: alles mit size / len nochmal überprüfen
    float information_loss_given_t(Item item, HashMap<String, Range<Float>> global_ranges){
        /*Calculates the information loss upon adding <item> into this cluster

        Args:
        item: The tuple to calculate information loss based on
        global_ranges: The globally known ranges for each attribute

        Returns: The information loss given that we insert item into this cluster*/
        float loss = 0F;

        // For each range, check if <item> would extend it
        Range <Float> updated = null;
        for (Map.Entry<String, Range<Float>> header: this.ranges.entrySet()){
            Range <Float> global_range = global_ranges.get(header.getKey());
            updated = Range.between((Math.min(header.getValue().getMinimum(), item.data.get(header.getKey()))), Math.max(header.getValue().getMaximum(), item.data.get(header.getKey())));
            loss += this.utils.range_information_loss(updated, global_range);
        }
        return loss;
    }


    float information_loss_given_c(Cluster cluster, HashMap<String, Range<Float>> global_ranges) {
       /* Calculates the information loss upon merging <cluster> into this cluster

        Args:
            cluster: The cluster to calculate information loss based on
            global_ranges: The globally known ranges for each attribute

        Returns: The information loss given that we merge cluster with this cluster*/
        float loss = 0F;

        // For each range, check if <item> would extend it
        Range <Float> updated = null;
        for (Map.Entry<String, Range<Float>> header: this.ranges.entrySet()){
            Range <Float> global_range = global_ranges.get(header.getKey());
            updated = Range.between(Math.min(header.getValue().getMinimum(), cluster.ranges.get(header.getKey()).getMinimum()),Math.max(header.getValue().getMaximum(),cluster.ranges.get(header.getKey()).getMinimum()));
            loss += this.utils.range_information_loss(updated, global_range);
        }
        return loss;
    }

    float information_loss(HashMap<String, Range<Float>> global_ranges){
        /*Calculates the information loss of this cluster

        Args:
        global_ranges: The globally known ranges for each attribute

        Returns: The current information loss of the cluster*/
        float loss = 0F;

        // For each range, check if <item> would extend it
        Range <Integer> updated = null;
        for (Map.Entry<String, Range<Float>> header: this.ranges.entrySet()){
            Range <Float> global_range = global_ranges.get(header.getKey());
            loss += this.utils.range_information_loss(header.getValue(), global_range);
        }
        return loss;
    }

    // TODO: implement difference Mehtod of Range
    // implement difference
    float distance(Item other){
        /*Calculates the distance from this tuple to another

        Args:
        other: The tuple to calculate the distance to

        Returns: The distance to the other tuple*/
        float total_distance = 0;
        for (Map.Entry<String, Range<Float>> header: this.ranges.entrySet()) {
            total_distance += Math.abs(this.utils.range_difference(other.data.get(header.getKey())));
        }
        return total_distance;
    }

    // TODO: implemtation of this function
    boolean within_bounds(Item item){
       /* Checks whether a tuple is within all the ranges of the this
        cluster, eg. would cause no information loss on being entered.

                Args:
        item: The tuple to perform bounds checking on

        Returns: Whether or not the tuple is within the bounds of the cluster*/

        return true;
    }


}