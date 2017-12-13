package simpledb;

import java.io.Serializable;
import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

import sun.security.pkcs.ParsingException;

import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class MaterializeView  {

    private static final long serialVersionUID = 1L;
    private BufferPool bp;
    private ArrayList<Tuple> materialized_subpaths = new ArrayList<Tuple>();
    MaterializeViewUtil mv_util = new MaterializeViewUtil();

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public MaterializeView()  {
        // Load table containing information on materialized subpath queries into the catalog
        Database.getCatalog().loadSubPathMaterializedViews();
        String subpaths_table_name = "mv_subpaths";
        this.bp = Database.getBufferPool();
        try {
            int table_id = Database.getCatalog().getTableId(subpaths_table_name);
            SeqScan ss = new SeqScan(new TransactionId(), table_id, "");
            ss.open();
            while(ss.hasNext()) {
                Tuple t = ss.next();
                materialized_subpaths.add(t);
            }
        } catch(DbException e) {
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            e.printStackTrace();
        }
    }
    

    public SeqScan get_mv_subpaths(String file_name) {
        int tableid = Database.getCatalog().getTableId(file_name);
        SeqScan ss = new SeqScan(new TransactionId(), tableid, "");
        return ss;
    }

    public void add_mv_subpaths(String file_name, OpIterator p, int num_paths) {
        TransactionId tid = new TransactionId();        
        try {
            int tableid = Database.getCatalog().getTableId(file_name);
            Insert ins = new Insert(tid, p, tableid);
            ins.open();
            Tuple numadded = ins.fetchNext();
            // p.open();
            // while(p.hasNext()) {
            //     Tuple t = p.next();
            //     int current_path_no = ((IntField)t.getField(0)).getValue();
            //     t.setField(0, new IntField(current_path_no + num_paths));
            //     bp.insertTuple(tid, tableid, t);            
            // }
            bp.transactionComplete(tid, true);
        } catch(DbException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            // bp.transactionComplete(tid, false);
            e.printStackTrace();
        }
    }

    public String add_mv_subpaths(OpIterator p) {
        // Transaction tid = new TransactionId();
        try {
            // We really just need to create a new file and sae it to mv file and disk
            Random r = new Random();
            Integer table_name_int = r.nextInt(10000000);
            String table_name = table_name_int.toString();
            TupleDesc td = p.getTupleDesc();
            HeapFile hf = Utility.createEmptyHeapFile(table_name, td);
            Database.getCatalog().addTable(hf, table_name);
            // If this is new we can't have current subpaths
            add_mv_subpaths(table_name, p, 0);
            return table_name;
        } catch(IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    public String create_new_mvsubpaths(OpIterator p, String query, int pgno, int tupno, int numpaths) {
        try {
            System.out.println("Creating new subpaths");
            Random r = new Random();
            Integer table_name_int = r.nextInt(10000000);
            String table_name = table_name_int.toString();
            TupleDesc td = p.getTupleDesc();
            HeapFile hf = Utility.createEmptyHeapFile(table_name, td);
            Database.getCatalog().addTable(hf, table_name);
            // If this is new we can't have current subpaths
            add_mv_subpaths(table_name, p, 0);
            // Now let's add to the mv_subpaths catalog file
            // And to the other thing
            String subpaths_table_name = "mv_subpaths";
            int table_id = Database.getCatalog().getTableId(subpaths_table_name);
            TupleDesc mv_td = mv_util.getSubPathMVTD();
            Tuple t = mv_util.createSubPathTuple(table_name, query, pgno,tupno,numpaths);

            // Insert into table and add to catalog cache
            TransactionId tid = new TransactionId();
            bp.insertTuple(tid, table_id, t);
            bp.transactionComplete(tid, true);
            System.out.println("We did something");
            Database.getCatalog().add_mv(t);
            return table_name;
        } catch(IOException e) {
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            e.printStackTrace();
        } catch(DbException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Graph Parser should instantiate it with nothing

    // private Pair<Integer,Integer> writeMaterializedTable() throws DbException, TransactionAbortedException, IOException {
    //     HeapFile hf = Utility.createEmptyHeapFile(table_name, this.td);
    //     Database.getCatalog().addTable(hf, table_name);
    //     System.out.println("Table Name from Id: " + Database.getCatalog().getTableId(table_name));
    //     int pgno = 0;
    //     int tupno = 0;
    //     query_result.open();
    //     while(query_result.hasNext()) {
    //         Tuple next_tuple_result = query_result.next();
    //         int tuple_pg_no = next_tuple_result.getRecordId().getPageId().getPageNumber();
    //         int tuple_no = next_tuple_result.getRecordId().getTupleNumber();
    //         if (tuple_pg_no > pgno || (tuple_pg_no == tupno && tuple_no > tupno)) {
    //             pgno = tuple_pg_no;
    //             tupno = tuple_no;
    //         }
    //         bp.insertTuple(tid, hf.getId(), next_tuple_result);
    //     }
    //     return new Pair<Integer,Integer>(pgno, tupno);
    // }

    // private Pair<Integer,Integer> writeOutput(HeapFile hf) throws DbException, TransactionAbortedException, IOException {
    //     int pgno = 0;
    //     int tupno = 0;
    //     query_result.open();
    //     while(query_result.hasNext()) {
    //         Tuple next_tuple_result = query_result.next();
    //         int tuple_pg_no = next_tuple_result.getRecordId().getPageId().getPageNumber();
    //         int tuple_no = next_tuple_result.getRecordId().getTupleNumber();
    //         if (tuple_pg_no > pgno || (tuple_pg_no == tupno && tuple_no > tupno)) {
    //             pgno = tuple_pg_no;
    //             tupno = tuple_no;
    //         }
    //         bp.insertTuple(tid, hf.getId(), next_tuple_result);
    //     }
    //     return new Pair<Integer,Integer>(pgno, tupno);
    // }

    // public void materializeSubPaths(String node_name, String edge_name, Field start_node_val,
    //                                 Field target_node_val, int val_field, Predicate.Op target_op,
    //                                 Predicate.Op start_op
    // ){
    //     try {



    //         int most_recent_pg = writeMaterializedTable();
    //         Tuple subpath_tuple = MaterializeViewUtil.createSubPathTuple(table_name, node_name, edge_name, start_node_val, target_node_val, val_field, target_op, start_op);
    //         createMaterializedDFSTable(subpath_tuple.getTupleDesc(), mv_subpaths_table_name);
    //         // Now we need to add that tuple
    //         int mv_tableid = Database.getCatalog().getTableId(mv_subpaths_table_name);
    //         bp.insertTuple(tid, mv_tableid, subpath_tuple);
    //         bp.transactionComplete(tid, true);            
    //     } catch(IOException e) {
    //         e.printStackTrace();
    //         System.out.println("Brutal Failure");
    //     } catch(DbException e) {
    //         e.printStackTrace();
    //     } catch(TransactionAbortedException e) {
    //         e.printStackTrace();
    //     }
    // }

    public void materializeSubPaths(HeapFile mv_table, String node_name, String edge_name, Field start_node_val,
                                    Field target_node_val, int val_field, Predicate.Op target_op,
                                    Predicate.Op start_op
    ){
        // Here we know the table_name so we should just use that because we know it exists.

    
    }

    // public SeqScan getMaterializedSubPaths(String node_name, String edge_name, Field start_node_val,
    //                                           Field target_node_val, int val_field, Predicate.Op target_op,
    //                                           Predicate.Op start_op
    // ){
    //     try {
    //         boolean mv_table_exists = Database.getCatalog().tableExists(mv_subpaths_table_name);
    //         if (!mv_table_exists) { 
    //             return null;
    //         }
    //         int file_id = Database.getCatalog().getTableId(mv_subpaths_table_name);
    //         HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(file_id);
    //         Tuple expected_tuple = createSubPathTuple("placeholder", node_name, edge_name, start_node_val, target_node_val, val_field, target_op, start_op);
    //         SeqScan ss = new SeqScan(tid, file_id, "");
    //         ss.open();
    //         while (ss.hasNext()) {
    //             Tuple t = ss.next();
    //             boolean tuple_matches = true;
    //             for (int i=1;i<t.getTupleDesc().numFields();i++) {
    //                 if (!t.getField(i).equals(expected_tuple.getField(i))) {
    //                     tuple_matches = false;
    //                 }
    //             } if (tuple_matches) {
    //                 int mv_table_id = Database.getCatalog().getTableId(t.getField(0).toString());
    //                 return new SeqScan(tid, mv_table_id, "");
    //             }
                
    //         }
    //         return null;
    //     } catch (DbException e) {
    //         e.printStackTrace();
    //     } catch (TransactionAbortedException e) {
    //         e.printStackTrace();
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    //     return null;
    // }

}