package com.schwartech.accumulo.ext.model;

import com.schwartech.accumulo.ext.comparators.DocumentDocumentIndexComparator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import play.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by jeff on 3/20/14.
 */
public class DocumentResultSet {
    public Map<Text, Document> rowDocuments;

    public DocumentResultSet() {
        rowDocuments = new HashMap<Text, Document>();
    }

    public void add(Key key, Value value) {
        Document d = get(key.getRow());

        if (d == null) {
            d = new Document();
        }

        d.add(key, value);

        rowDocuments.put(d.rowKey, d);
    }

    public Document get(Text key) {
        return rowDocuments.get(key);
    }

    public Document get(String key) {
        return get(new Text(key));
    }

    public Map<Text, Document> sort(List<Range> colQuals) {
        Map<Text, Document> sortedMap =  new TreeMap<Text, Document>(new DocumentDocumentIndexComparator(colQuals));
//        Logger.info("keys pre-sort : "+dirs.rowDocuments.keySet());
        for (Range range : colQuals) {
//            Logger.info("key :"+ key.toString() +" | rowKey : "+dirs.get(key.toString()).rowKey);
//            Logger.info("range row key : "+dirs.rowDocuments.get(key).getColQualifiers());
            Logger.info("key : "+range.getStartKey().getRow()+" | Value : "+get(range.getStartKey().getRow()));
            sortedMap.put(range.getStartKey().getRow(), get(range.getStartKey().getRow()));
        }
        rowDocuments = sortedMap;
//        Logger.info("keys : "+dirs.rowDocuments.keySet());
        return sortedMap;
    }
}