package com.schwartech.accumulo.ext.comparators;

import com.schwartech.accumulo.ext.model.Document;
import com.schwartech.accumulo.ext.model.DocumentIndexResultSet;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import play.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Created by Sergio on 5/2/14.
 */
public class DocumentDocumentIndexComparator implements Comparator<Text> {
    private List<Range> sortedRanges;

    public DocumentDocumentIndexComparator(List<Range> sortedRanges) {
        this.sortedRanges = sortedRanges;
    }

    @Override
    public int compare(Text doc1, Text doc2) {
        int idx1 = sortedRanges.indexOf(new Range(doc1));
        int idx2 = sortedRanges.indexOf(new Range(doc2));

        Logger.info(("Idx1: " + idx1 + ", Idx2: " + idx2));

        return new Integer(idx1).compareTo(new Integer(idx2));
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }
}
