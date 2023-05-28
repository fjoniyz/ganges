package com.ganges.lib.castleguard;
import java.util.*;
import com.ganges.lib.castleguard.utils.Utils;

public class Item {
    Map<String, Float> data;
    List<String> headers;
    Float sensitive_attr;
    Cluster parent;
    Utils util;

    public Item(Map <String, Float> data, List <String>  headers, String sensitive_attr){
        this.data  = data;
        this.headers = headers;
        this.sensitive_attr = data.get(sensitive_attr);
        this.parent = null;
    }
    float tuple_distance(Item other){
        /*Calculates the distance between the two tuples
        Args:
        other: The tuple to calculate the distance to
        Returns: The distance to the tuple*/
        return 0F;
    }

    void update_attribute(String header, Float value) {
        /*Updates a value in the tuple's data
        Args:
            header: The header to change
            value: The value to change to*/
        this.data.replace(header, value);
    }
}
