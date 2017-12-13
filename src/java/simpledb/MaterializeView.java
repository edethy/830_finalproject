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
        // Database.getCatalog().loadSubPathMaterializedViews();
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

    public int add_mv_subpaths(String file_name, OpIterator p, int num_paths) {
        System.out.println("Num Paths: " + num_paths);
        TransactionId tid = new TransactionId();        
        try {
            int tableid = Database.getCatalog().getTableId(file_name);
            int new_num_paths = num_paths;
            p.rewind();
            while(p.hasNext()) {
                Tuple t = p.next();
                // System.out.println("Inserting tuple" + t);
                int current_path_no = ((IntField)t.getField(0)).getValue();
                int new_num_p = current_path_no + num_paths;
                if (num_paths != 0) {
                    new_num_p++;
                }
                t.setField(0, new IntField(new_num_p));
                bp.insertTuple(tid, tableid, t);
                if (new_num_p > new_num_paths) {
                    new_num_paths = new_num_p;
                }
            }
            bp.transactionComplete(tid, true);
            return new_num_paths;
        } catch(DbException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void add_new_mvsubpaths(String table_name, OpIterator p, String query, int pgno, int tupno, int numpaths) {

        try {
            int new_path_num = add_mv_subpaths(table_name, p, numpaths);    
            String subpaths_table_name = "mv_subpaths";
            int table_id = Database.getCatalog().getTableId(subpaths_table_name);
            TransactionId tid = new TransactionId();

            Tuple t = Database.getCatalog().get_mv_by_table_name(table_name);
            if (t == null) {
                TupleDesc mv_td = mv_util.getSubPathMVTD();
                t = mv_util.createSubPathTuple(table_name, query, pgno,tupno,new_path_num);
                bp.insertTuple(tid, table_id, t);
            } else {
                t.setField(2, new IntField(pgno));
                t.setField(3, new IntField(tupno));
                t.setField(4, new IntField(new_path_num));
                bp.updateTuple(tid, table_id, t);
            }
            bp.transactionComplete(tid, true);
            Database.getCatalog().add_mv(t);
        } catch (DbException e) {

        } catch (IOException e) {

        } catch (TransactionAbortedException e) {

        }

    }

    public String create_new_mvsubpaths(OpIterator p, String query, int pgno, int tupno, int numpaths) {
        try {
            System.out.println("Creating New MaterializedView Table for Query: " + query);
            Random r = new Random();
            Integer table_name_int = r.nextInt(10000000);
            String table_name = "subpath_"+table_name_int.toString();
            TupleDesc td = p.getTupleDesc();
            HeapFile hf = Utility.createEmptyHeapFile(table_name, td);
            Database.getCatalog().addTable(hf, table_name);
            add_new_mvsubpaths(table_name, p, query, pgno, tupno, numpaths);
            return table_name;
        } catch(IOException e) {
            e.printStackTrace();
        } 
        return null;
    }


}