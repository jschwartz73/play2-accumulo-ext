package com.schwartech.accumulo.ext.helpers;

import com.schwartech.accumulo.Accumulo;
import com.schwartech.accumulo.ext.model.DocumentIndexResultSet;
import com.schwartech.accumulo.ext.model.DocumentResultSet;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * Created by jeff on 3/24/14.
 */
public class DataHelper {

    public static DocumentResultSet query(String table, Authorizations auths, Set<Range> ranges, Map<String, String> columnsToFetch) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        DocumentResultSet drs = new DocumentResultSet();

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();

            BatchScanner scanner = connector.createBatchScanner(table, auths, Accumulo.getNumThreads());

            if (!ranges.isEmpty()) {
                scanner.setRanges(ranges);
            }

            for (Map.Entry<String, String> entry : columnsToFetch.entrySet()) {
                scanner.fetchColumn(new Text(entry.getKey()), new Text(entry.getValue()));
            }

            for (Map.Entry<Key,Value> entry : scanner) {
                drs.add(entry.getKey(), entry.getValue());
            }

            scanner.close();
        } finally {
            Accumulo.closeConnector(connector);
        }

        return drs;
    }

    public static DocumentIndexResultSet queryIndex(String table, Authorizations auths, Range range) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        DocumentIndexResultSet dirs = new DocumentIndexResultSet();

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();

            Scanner indexScanner = connector.createScanner(table, auths);

            indexScanner.setRange(range);

            for (Map.Entry<Key,Value> entry : indexScanner) {
                dirs.add(entry.getKey(), entry.getValue());
            }

            indexScanner.close();
        } finally {
            Accumulo.closeConnector(connector);
        }


        return dirs;
    }

    public static void deleteRange(String table, Authorizations auths, Range range) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            BatchDeleter deleter = connector.createBatchDeleter(table, auths, Accumulo.getNumThreads(), Accumulo.getDefaultWriterConfig());
            List<Range> ranges = new ArrayList<Range>();
            ranges.add(range);
            deleteRanges(table, auths, ranges);
        } finally {
            Accumulo.closeConnector(connector);
        }
    }

    public static void deleteRanges(String table, Authorizations auths, Collection<Range> ranges) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector connector = null;
        try {
            connector = Accumulo.getConnector();
            BatchDeleter deleter = connector.createBatchDeleter(table, auths, Accumulo.getNumThreads(), Accumulo.getDefaultWriterConfig());
            deleter.setRanges(ranges);
            deleter.delete();
            deleter.close();
        } finally {
            Accumulo.closeConnector(connector);
        }
    }
}