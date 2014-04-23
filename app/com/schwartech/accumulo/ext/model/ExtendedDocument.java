package com.schwartech.accumulo.ext.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Sergio on 4/2/14.
 */
public class ExtendedDocument {
    private Document doc;

    public Document getDoc() {
        return doc;
    }

    private ExtendedDocument(Document document) {
        this.doc = document;

    }

    public static ExtendedDocument toExt(Document doc) {
        return new ExtendedDocument(doc);
    }

    private String getColQualifier(String colQual, int index) {
        return colQual + "[" + index + "]";
    }

    public List<String> getColQualAsArray(String colFam, String colQual) {
        List<String> array = new ArrayList<String>();
        int index = 0;
        String val = doc.getValueAsString(colFam, getColQualifier(colQual, index));
        while (val != null && !val.isEmpty()) {
            array.add(val);
            index++;
            val = doc.getValueAsString(colFam, getColQualifier(colQual, index));
        }

        return array;
    }

    public String getColQual(String key) {
        int idx = key.lastIndexOf(':');
        if (idx >= 0) {
            return key.substring(idx+1);
        } else {
            return key;
        }
    }

    private String getColFamily(String key) {
        int idx = key.lastIndexOf(':');
        if (idx >= 0) {
            return key.substring(0, idx);
        } else {
            return key;
        }
    }

    public Set<String> getColFamPrefixAsArray(String colFamPrefix) {
        Set<String> set = new HashSet<String>();

        for (String key: doc.getKeys()) {
            if (key.startsWith(colFamPrefix)) {
                set.add(getColFamily(key));
            }
        }

        return set;
    }

}
