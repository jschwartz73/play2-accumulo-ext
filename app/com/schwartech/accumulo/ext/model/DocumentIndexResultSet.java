package com.schwartech.accumulo.ext.model;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * Created by jeff on 3/20/14.
 */
public class DocumentIndexResultSet {
    public Map<Text, DocumentIndex> rowDocuments;

    public DocumentIndexResultSet() {
        rowDocuments = new TreeMap<Text, DocumentIndex>();
    }

    public Boolean add(Key key, Value value) {
        DocumentIndex d = getByRow(key.getRow());

        if (d == null) {
            d = new DocumentIndex();
        }

        Boolean contains = d.add(key, value);

        rowDocuments.put(d.rowKey, d);

        return contains;
    }

    private DocumentIndex getByRow(Text key) {
        return rowDocuments.get(key);
    }

    public DocumentIndex get(String key) {
        return getByRow(new Text(key));
    }

    public List<Range> getColQualifiers() {
        List<Range> ranges = new LinkedList<Range>();

        for (Text t : rowDocuments.keySet()) {
            ranges.addAll(rowDocuments.get(t).getColQualifiersAsRangeList());
        }

        return ranges;
    }

    public Set<Range> getColQualifiers(String colFamily) {
        Set<Range> ranges = new HashSet<Range>();

        for (Text t : rowDocuments.keySet()) {
            ranges.addAll(rowDocuments.get(t).getColQualifiersAsRangeList(colFamily));
        }

        return ranges;
    }
}