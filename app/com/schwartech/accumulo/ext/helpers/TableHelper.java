package com.schwartech.accumulo.ext.helpers;

import com.schwartech.accumulo.Accumulo;
import org.apache.accumulo.core.client.*;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by jeff on 3/24/14.
 */
public class TableHelper {

    public static boolean tableExists(String table) throws AccumuloSecurityException, AccumuloException {
        boolean tableExists = false;

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            tableExists = connector.tableOperations().exists(table);
        } finally {
            Accumulo.closeConnector(connector);
        }

        return tableExists;
    }

    public void deleteTable(String table) throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            connector.tableOperations().delete(table);
        } finally {
            Accumulo.closeConnector(connector);
        }
    }

    public static void createTable(String table) throws AccumuloSecurityException, AccumuloException, TableExistsException {
        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            connector.tableOperations().create(table);
        } finally {
            Accumulo.closeConnector(connector);
        }
    }

    public static SortedSet<String> getUserTables() throws AccumuloSecurityException, AccumuloException {
        SortedSet<String> tables = getAllTables();
        tables.remove("trace");
        tables.remove("!METADATA");
        return tables;
    }

    public static SortedSet<String> getAllTables() throws AccumuloSecurityException, AccumuloException {
        SortedSet<String> tables = new TreeSet<String>();

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            tables = connector.tableOperations().list();
        } finally {
            Accumulo.closeConnector(connector);
        }

        return tables;
    }
}