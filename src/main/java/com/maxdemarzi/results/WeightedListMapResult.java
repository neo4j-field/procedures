package com.maxdemarzi.results;

import java.util.List;
import java.util.Map;

public class WeightedListMapResult {
    public final List<Map<String,Object>> maps;
    public final Double weight;

    public WeightedListMapResult(List<Map<String,Object>> maps, Double weight) {
        this.maps = maps;
        this.weight = weight;
    }
}