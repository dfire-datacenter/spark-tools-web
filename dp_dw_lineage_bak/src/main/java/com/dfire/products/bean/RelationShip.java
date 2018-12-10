package com.dfire.products.bean;

import java.util.List;
import java.util.Map;

public class RelationShip {
    private long node1Id;
    private long node2Id;

    /**
     * from,hive
     */
    private String                    label;
    private Map<String, List<String>> propertyMap;

    public long getNode1Id() {
        return node1Id;
    }

    public void setNode1Id(long node1Id) {
        this.node1Id = node1Id;
    }

    public long getNode2Id() {
        return node2Id;
    }

    public void setNode2Id(long node2Id) {
        this.node2Id = node2Id;
    }

    public String getLable() {
        return label;
    }

    public void setLable(String label) {
        this.label = label;
    }

    public Map<String, List<String>> getPropertyMap() {
        return propertyMap;
    }

    public void setPropertyMap(Map<String, List<String>> propertyMap) {
        this.propertyMap = propertyMap;
    }
}
