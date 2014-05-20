package com.schwartech.accumulo.ext.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by jeff on 3/20/14.
 */

//TODO: JSS - this is getting mighty complicated for serializing and sending as JSON.
//TODO: JSS - This must be refactored
public class Document {
    private Text rowKey = null;

    // TODO: JSS - I'm not sure if we need the original data
//    private Map<Key, Value> originalData;

    //TODO: JSS - Refactor this to be better for sending over the wire.
    private Map<String, Value> data;

    public Document() {
//        originalData = new HashMap<Key, Value>();
        data = new HashMap<String, Value>();
    }

    public void add(Key key, Value value) throws IllegalArgumentException {
        if (rowKey == null) {
            rowKey = key.getRow();
        } else if (!key.getRow().equals(rowKey)) {
            throw new IllegalArgumentException("Cannot add this data.  Row keys are different: " + rowKey.toString() + "/" + key.getRow().toString());
        }

//        originalData.put(key, value);
        data.put(getDataKey(key.getColumnFamily(), key.getColumnQualifier()), value);
    }

    private String getDataKey(String colFamily, String colQualifier) {
        return colFamily + ":" + colQualifier;
    }

    private String getDataKey(Text colFamily, Text colQualifier) {
        return colFamily + ":" + colQualifier;
    }

    @JsonIgnore
    public Value getValue(String colFamily, String colQualifier) {
        Value value = data.get(getDataKey(colFamily, colQualifier));

        //TODO: JSS - this should return null as that is different than ""
        if (value == null) {
            value = new Value("".getBytes());
        }
        return value;
    }

    @JsonIgnore
    public Text getRowKey() {
        return this.rowKey;
    }

    public String getRowKeyAsText() {
        return this.rowKey.toString();
    }

    public void setRowKeyAsText(String key) {
        this.rowKey = new Text(key);
    }

    @JsonIgnore
    public Set<String> getKeys() {
        return data.keySet();
    }

    @JsonIgnore
    public String getValueAsString(String colFamilyQualifier) {
        int idx = colFamilyQualifier.indexOf(':');
        String fam = colFamilyQualifier.substring(0, idx);
        String qual = colFamilyQualifier.substring(idx+1);
        return getValueAsString(fam, qual);
    }

    @JsonIgnore
    public String getValueAsString(String colFamily, String colQualifier) {
        return getValue(colFamily, colQualifier).toString();
    }

    public Map<String, String> getDataMap() {
        Map<String, String> map = new HashMap<String, String>();

        for (Map.Entry<String, Value> e : data.entrySet()) {
            map.put(e.getKey(), e.getValue().toString());
        }

        return map;
    }

    public void setDataMap(Map<String, String> map) {
        data = new HashMap<String, Value>();

        for (Map.Entry<String, String> e : map.entrySet()) {
            data.put(e.getKey(), new Value(e.getValue().getBytes()));
        }
    }
}
