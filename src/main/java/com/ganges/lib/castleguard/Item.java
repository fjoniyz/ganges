package com.ganges.lib.castleguard;
import java.util.*;

public class Item {
    Map<String, Float> data;
    List<String> headers;
    Float sensitive_attr;
    // TODO: is this Cluster or Item Type?
    Object parent = null;

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

        Float error = Math.abs(this.data.get(this.headers) - other.data.get(this.headers));
        // TODO: noch Mean Value darauf anwenden
        Float mean_squared_error = (float) Math.pow(error, 2);
        return (float) Math.sqrt(mean_squared_error);
    }

    void update_attribute(String header, Float value) {
        /*Updates a value in the tuple's data
        Args:
            header: The header to change
            value: The value to change to*/
        this.data.replace(header, value);
    }
}
