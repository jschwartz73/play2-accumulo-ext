package com.schwartech.accumulo.ext.comparators;

import com.schwartech.accumulo.ext.model.Document;

import java.util.Comparator;
import java.util.List;

/**
 * Created by Sergio on 4/9/14.
 */
public class DocumentComparator implements Comparator<Document> {
    List<String> keys;

    public DocumentComparator(List<String> keys) {
        this.keys = keys;
    }

    @Override
    public int compare(Document document1, Document document2) {
        for (String key : keys) {
            if (!document1.getValueAsString(key).equals(document2.getValueAsString(key))) {
                return document1.getValueAsString(key).compareToIgnoreCase(document2.getValueAsString(key));
            }
        }
        return document1.getValueAsString(keys.get(0)).compareToIgnoreCase(document2.getValueAsString(keys.get(0)));
    }
}