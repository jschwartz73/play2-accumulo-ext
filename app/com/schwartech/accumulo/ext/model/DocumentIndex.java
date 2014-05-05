package com.schwartech.accumulo.ext.model;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * Created by jeff on 3/20/14.
 */
public class DocumentIndex {
    public Text rowKey = null;

    // TODO: JSS - I'm not sure if we need the original data
//    Map<Key, Value> originalData;
    Map<String, List<String>> indexData;

    public DocumentIndex() {
//        originalData = new HashMap<Key, Value>();
        indexData = new HashMap<String, List<String>>();
    }

    public Boolean add(Key key, Value value) throws IllegalArgumentException {
        Boolean contains = true;
        if (rowKey == null) {
            rowKey = key.getRow();
        } else if (!key.getRow().equals(rowKey)) {
            throw new IllegalArgumentException("Cannot add this data.  Row keys are different: " + rowKey.toString() + "/" + key.getRow().toString());
        }

//        originalData.put(key, value);

        String colFamKey = key.getColumnFamily().toString();
        String colQualKey = key.getColumnQualifier().toString();

        for (String text  : indexData.keySet()) {
            if (indexData.get(text).contains(colQualKey)){
                contains = false;
            }
        }

        if (!indexData.containsKey(colFamKey)) {
            indexData.put(colFamKey, new ArrayList<String>());
        }

        List<String> colQualifiers = indexData.get(colFamKey);
        colQualifiers.add(colQualKey);

        indexData.put(colFamKey, colQualifiers);

        return contains;
    }

    public Set<String> getColQualifiers() {
        return indexData.keySet();
    }

    public List<String> getColQualifiers(String colFamily) {
        String key = colFamily.toString();
        if (indexData.containsKey(key)) {
            return indexData.get(key);
        } else {
            return new ArrayList<String>();
        }
    }

    public Set<Range> getColQualifiersAsRangeList() {
        Set<Range> ranges = new LinkedHashSet<Range>();

        for (String key : indexData.keySet()) {
            for (String r : indexData.get(key)) {
                ranges.add(new Range(new Text(r)));
            }
        }

        return ranges;
    }

    public Set<Range> getColQualifiersAsRangeList(String colFamily) {
        String key = colFamily.toString();
        Set<Range> ranges = new HashSet<Range>();
        for (String r : indexData.get(key)) {
            ranges.add(new Range(new Text(r)));
        }
        return ranges;
    }
}
