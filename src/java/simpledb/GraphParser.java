package simpledb;

import Zql.*;
import simpledb.DFSJoin;
import simpledb.SubPaths;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import java.util.Arrays;

public class GraphParser {

    // materialize view as newtable get paths 0 = facebook.com to 1 = db.csail.com using edge node;

    int start_field_index = 0;
    int start_pred_index = 1;     
    int start_field_value_index = 2;
    int end_field_index = 4;
    int end_field_pred_index = 5;
    int end_field_value_index = 6;
    int edge_table_index = 8;
    int node_table_index = 9; 

    /***
     * This class needs to support parsing the following
     *      1. get paths  start.fieldX PRED Yand end.fieldZ PRED Z USING edge_table node_table
     *      2. materialize view X AS (SELECT STATEMENT)
     *             - Return SELECT statement to be executed + do other stuff
     *      3. SELECT X from (get paths)
     *      4. Get SubPaths 
     */

    static boolean explain = false;

    public void processQuery(String s) {
        String[] split_query = s.split(" ");
        if (split_query[0].equalsIgnoreCase("get") && split_query[1].equals("paths")) {
            System.out.println("Query: " + s);
            parseAndRunDFSGraphPath(split_query);
            
        } else if (split_query[0].equalsIgnoreCase("materialize") && split_query[1].equalsIgnoreCase("view")) {
            String table_name = split_query[3];
            System.out.println("Table Name: " + table_name);
            String select_query = String.join(" ", Arrays.copyOfRange(split_query, 4, split_query.length));
            System.out.println("Select query from materialize view: " + select_query);
            if (split_query[4].equalsIgnoreCase("get") && split_query[5].equalsIgnoreCase("paths")) {
                parseAndRunDFSGraphPath(select_query.split(" "));
                return;
            }
            select_query = select_query.substring(0, select_query.length() - 1).trim() + ";";
            // need to pass select query to parser or sometihng
            // byte[] statementBytes = select_query.getBytes("UTF-8");
            Parser p = new Parser();
            Query q = p.processNextStatementAndReturn(select_query);
            try {
                q.start();
                while (q.hasNext()) {
                    Tuple next_tup = q.next();
                    System.out.println("Next Tuple: " + next_tup);
                }
            } catch(IOException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            } catch(TransactionAbortedException e) {
                e.printStackTrace();
            }
        } else if (split_query[0].equalsIgnoreCase("get") && split_query[1].equalsIgnoreCase("subpaths")) {
            System.out.println("Parsing and Running SubPathQuery");
            parseAndRunSubPathQuery(split_query);
        }
    }

    public OpIterator getGraphPathFromQuery(String[] split_query) {
        split_query = Arrays.copyOfRange(split_query, 2, split_query.length);
        String start_field = split_query[start_field_index];
        String start_field_pred = split_query[start_pred_index];
        String start_field_value = split_query[start_field_value_index];
        String end_field = split_query[end_field_index];
        String end_field_pred = split_query[end_field_pred_index];
        String end_field_value = split_query[end_field_value_index];

        System.out.println("Start Field: " + start_field + " Field Pred: " + start_field_pred);
        return null;
    }

    private void parseAndRunDFSGraphPath(String[] split_query) {

        try {
            split_query = Arrays.copyOfRange(split_query, 2, split_query.length);
            System.out.println("Split Query: " + split_query);
            String start_field = split_query[start_field_index];
            String start_field_pred = split_query[start_pred_index];
            String start_field_value = split_query[start_field_value_index];
            String end_field = split_query[end_field_index];
            String end_field_pred = split_query[end_field_pred_index];
            String end_field_value = split_query[end_field_value_index];
            String edge_table_name = split_query[edge_table_index];
            String node_table_name = split_query[node_table_index];
            System.out.println(
                "Start Field: " + start_field + " \n" + 
                "Start Field Pred: " + start_field_pred + "\n" +
                "Start Field Value: " + start_field_value + "\n" +
                "End Field: " + end_field + "\n" + 
                "End Field Pred: " + end_field_pred + "\n" +
                "End Field Value:  " + end_field_value + " \n" +
                "Edge Table Name: " + edge_table_name + "\n" +
                "Node Table Name: " + node_table_name
                );
    
            Field start_node_value = new StringField(start_field_value);
            Field target_node_value = new StringField(end_field_value);
            Predicate.Op start_field_op = Parser.getOp(start_field_pred);
            Predicate.Op target_node_op = Parser.getOp(end_field_pred);
            int node_pk_field = 0;
            int target_node_field = Integer.parseInt(end_field);
            int target_node_join_field = 1;
            // runDFSQuery(node_table_name, edge_table_name, start_node_value, node_pk_field, target_node_op, target_node_value, target_node_field, target_node_join_field);
            runBFSQuery(node_table_name, edge_table_name, target_node_value, start_node_value, 0, 1); 
        } catch(simpledb.ParsingException e) {
            e.printStackTrace();
        }
    }

    /***
     * 
     * OpIterator nodes, OpIterator edges, 
     * Field start_node_value, 
     * int node_pk_field, Predicate.Op target_node_op, 
     * Field target_node_field_value, int target_node_field, 
     * int target_node_join_field
     * 
     */

/***
 * 
 * 
 *     public BFS(Field start_node_id, Field end_node_id, OpIterator edges, 
 * int start_node_field, int target_node_field) {

 */
    private void runBFSQuery(String nodes_name, String edges_name, Field end_node_id, Field start_node_id,
                             int start_node_field, int target_node_field
    ){
        int nodes_tableid = Database.getCatalog().getTableId(nodes_name);
        int edges_tableid = Database.getCatalog().getTableId(edges_name);

        TransactionId tid = new TransactionId();
        SeqScan nodes = new SeqScan(tid, nodes_tableid, "");
        SeqScan edges = new SeqScan(tid, edges_tableid, "");
        System.out.println("Running BFS Query");
        BFS b = new BFS(start_node_id, end_node_id, edges, start_node_field, target_node_field);
        System.out.println("B Paths: " + b.getPaths());
    } 


    private void runDFSQuery(String nodes_name, String edges_name, Field start_node_value, int node_pk_field,
                            Predicate.Op target_node_op, Field target_node_field_value, int target_node_field,
                            int target_node_join_field
    ){
        int nodes_tableid = Database.getCatalog().getTableId(nodes_name);
        int edges_tableid = Database.getCatalog().getTableId(edges_name);

        TransactionId tid = new TransactionId();
        SeqScan nodes = new SeqScan(tid, nodes_tableid, "");
        SeqScan edges = new SeqScan(tid, edges_tableid, "");

        DFSJoin d = new DFSJoin(nodes, edges, start_node_value, node_pk_field, target_node_op, target_node_field_value, target_node_field, target_node_join_field);

    }

    private void parseAndRunSubPathQuery(String[] split_query) {
        /***
         * get subpaths from start PRED value to end PRED value using nodes n edges e on val_field
         */

        // Here we are just parsing and then running
        try {
            System.out.println("Parsing SubPath Query");
            String start_p = split_query[4];
            String start_val_str = split_query[5];
            String end_p = split_query[8];
            String end_val_str = split_query[9];
            String nodes_name = split_query[12];
            String edges_name = split_query[14];
            String val_field = split_query[16];
    
            Predicate.Op start_node_op = Parser.getOp(start_p);
            Predicate.Op target_node_op = Parser.getOp(end_p);
            int value_field = Integer.parseInt(val_field);
            Field start_val = null;
            Field end_val = null;
            if (value_field != 4) {
                start_val = new IntField(Integer.parseInt(start_val_str));
                end_val = new IntField(Integer.parseInt(end_val_str));
            } else {
                start_val = new StringField(start_val_str);
                end_val = new StringField(end_val_str);
            }
            System.out.println("Running SubPaths Query");
            runSubPathQuery(nodes_name, edges_name, start_val, end_val,value_field, target_node_op, start_node_op);

        } catch(simpledb.ParsingException e) {
            e.printStackTrace();
        }
    }

    private void runSubPathQuery(String nodes, String edges, Field start_node_value, Field target_node_value,
                                int value_field, Predicate.Op target_node_op, Predicate.Op start_node_op
    ){
        try {
            TransactionId tid = new TransactionId();
            int node_table_id = Database.getCatalog().getTableId(nodes);
            int edge_table_id = Database.getCatalog().getTableId(edges);
            SeqScan node_iterator = new SeqScan(tid, node_table_id, "");
            SeqScan edge_iterator = new SeqScan(tid, edge_table_id, "");
    

            // First look up in materialized view table whether or not it exists

            SubPaths subpaths = new SubPaths(node_iterator, edge_iterator, start_node_value, target_node_value, value_field, target_node_op, start_node_op);
            subpaths.open();
            while(subpaths.hasNext()) {
                Tuple path_tuple = subpaths.next();
                System.out.println("Path Tuple: " + path_tuple);
            }
        } catch(DbException e) {
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            e.printStackTrace();
        }
    }
}