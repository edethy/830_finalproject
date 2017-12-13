package simpledb;

import Zql.*;
import simpledb.DFSJoin;
import simpledb.SubPaths;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import java.util.Arrays;
import java.util.Random;

public class GraphParser {

    //boolean useMaterialization = false;

    int start_field_index = 0;
    int start_pred_index = 1;     
    int start_field_value_index = 2;
    int end_field_index = 4;
    int end_field_pred_index = 5;
    int end_field_value_index = 6;
    int edge_table_index = 8;
    int node_table_index = 9; 

    public String mv_subpaths_table_name = "mv_subpaths";    
    private long start_time;

    MaterializeView mv  = new MaterializeView();

    String subpath_query = "get subpaths from start %s %s to end %s %s using nodes %s edges %s on %s %s ;";

    static boolean explain = false;

    public void processQuery(String s) {
        this.start_time = System.currentTimeMillis();
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
        long end_time = System.currentTimeMillis();
        long time_diff = end_time - start_time;
        System.out.println("Total Time Spent Running the Query:: " + time_diff + "ms");
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
            // System.out.println("Parsing SubPath Query");
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
            boolean useMaterialization = Boolean.parseBoolean(split_query[17]);
            System.out.println("Use Materialization: " + useMaterialization);
            if (useMaterialization) {
                runSubPathQueryWithMaterialization(nodes_name, edges_name, start_val, end_val,value_field, target_node_op, start_node_op);
            } else {
                runSubPathQuery(nodes_name, edges_name, start_val, end_val,value_field, target_node_op, start_node_op);
            }

        } catch(simpledb.ParsingException e) {
            e.printStackTrace();
        }
    }

    private OpIterator runSubPathQueryWithMaterialization(String nodes, String edges, Field start_node_value, Field target_node_value,
                                int value_field, Predicate.Op target_node_op, Predicate.Op start_node_op
    ){
            TransactionId tid = new TransactionId();

            int node_table_id = Database.getCatalog().getTableId(nodes);
            int edge_table_id = Database.getCatalog().getTableId(edges);


            List<Object> args = new ArrayList<Object>();
            args.add(Parser.getOpString(start_node_op));
            args.add(start_node_value.toString());
            args.add(Parser.getOpString(target_node_op));
            args.add(target_node_value.toString());
            args.add(nodes);
            args.add(edges);
            args.add(value_field);
            args.add(true);            

            String mv_query = String.format(subpath_query,args.toArray());
            Tuple mv_table = Database.getCatalog().get_mv(mv_query);

            int start_pg_no = 0;
            int start_tup_no = 0;
            int path_num = 0;
            String table_name = "";
            if (mv_table != null) {
                table_name = mv_table.getField(0).toString();
                start_pg_no = ((IntField)mv_table.getField(2)).getValue();
                start_tup_no = ((IntField)mv_table.getField(3)).getValue()+1;
                path_num = ((IntField)mv_table.getField(4)).getValue();
            }

            SeqScan node_iterator = new SeqScan(tid, node_table_id, "");
            SeqScan edge_iterator = new SeqScan(tid, edge_table_id, "", start_pg_no);
            SubPaths subpaths = new SubPaths(node_iterator, edge_iterator, start_node_value, target_node_value, value_field, target_node_op, start_node_op, start_tup_no, start_pg_no);

            int latest_pg = subpaths.getLastPageNumber();
            int latest_tup = subpaths.getLastTupleNumber();
            int next_path_num = subpaths.getLatestPathIndex();
            
            if (mv_table != null) {
                mv.add_new_mvsubpaths(table_name, subpaths, mv_query, latest_pg, latest_tup, path_num);
            } else {
                table_name = mv.create_new_mvsubpaths(subpaths, mv_query, latest_pg, latest_tup, path_num);
            }
            int mv_finaltable_id = Database.getCatalog().getTableId(table_name);
            SeqScan ss_mv_table = new SeqScan(new TransactionId(), mv_finaltable_id, "");
            return ss_mv_table;

    }
    private OpIterator runSubPathQuery(String nodes, String edges, Field start_node_value, Field target_node_value,
                                int value_field, Predicate.Op target_node_op, Predicate.Op start_node_op
    ){
        TransactionId tid = new TransactionId();
        int node_table_id = Database.getCatalog().getTableId(nodes);
        int edge_table_id = Database.getCatalog().getTableId(edges);            

        SeqScan node_iterator = new SeqScan(tid, node_table_id, "");
        SeqScan edge_iterator = new SeqScan(tid, edge_table_id, "");

        SubPaths subpaths = new SubPaths(node_iterator, edge_iterator, start_node_value, target_node_value, value_field, target_node_op, start_node_op);
        return subpaths;
    }

    public SeqScan getMaterializedSubPaths(TransactionId tid, String node_name, String edge_name, Field start_node_val,
                                              Field target_node_val, int val_field, Predicate.Op target_op,
                                              Predicate.Op start_op
    ){
        try {
            String mv_subpaths_table_name = "mv/mv_subpaths";
            boolean mv_table_exists = Database.getCatalog().tableExists(mv_subpaths_table_name);
            if (!mv_table_exists) { 
                return null;
            }

            int file_id = Database.getCatalog().getTableId(mv_subpaths_table_name);
            HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(file_id);
            // Tuple expected_tuple = createSubPathTuple("placeholder", node_name, edge_name, start_node_val, target_node_val, val_field, target_op, start_op);
            SeqScan ss = new SeqScan(tid, file_id, "");
            ss.open();
            while (ss.hasNext()) {
                Tuple t = ss.next();
                boolean tuple_matches = true;
                // for (int i=1;i<t.getTupleDesc().numFields();i++) {
                //     if (!t.getField(i).equals(expected_tuple.getField(i))) {
                //         tuple_matches = false;
                //     }
                // } if (tuple_matches) {
                //     int mv_table_id = Database.getCatalog().getTableId(t.getField(0).toString());
                //     return new SeqScan(tid, mv_table_id, "");
                // }
                
            }
            return null;
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } 
        return null;

    }

}