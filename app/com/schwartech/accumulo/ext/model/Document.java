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
public class Document {
    public Text rowKey = null;

    // TODO: JSS - I'm not sure if we need the original data
//    private Map<Key, Value> originalData;

    //TODO: JSS - Refactor this to be better for sending over the wire.
    //TODO: JSS - Public may not be the best visibility
    public Map<String, Value> data;

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
}
