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

	private OpIterator query_result;
	private String query;
	private String table_name;
	private TupleDesc td;

    private String mv_query_table_name = "materialized_views";
    private String mv_dfsjoin_table_name = "mv_dfspaths";
    private String mv_subpaths_table_name = "mv_subpaths";

    private TupleDesc mv_td = new TupleDesc(new Type[] {Type.STRING_TYPE, Type.STRING_TYPE}, new String[] {"view_name", "view_query"});

    private HeapFile hf;

    private static final long serialVersionUID = 1L;

    private BufferPool bp;

    private TransactionId tid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public MaterializeView(String table_name, String query, OpIterator query_result)  {
    	this.query_result = query_result;
        this.query = query;
        this.table_name = table_name;
        this.td = query_result.getTupleDesc();
        this.bp = Database.getBufferPool();
        this.tid = new TransactionId();
    }

    public void materializeDFSJoinPaths(Field start_node_val, int node_pk_field, Predicate.Op target_node_op,
                                        Field target_node_field_value, int target_node_field, int target_node_join_field, String node_table_name,
                                        String edge_table_name) {

        String[] td_field_names = new String[] {"name","NodeTableName", "EdgeTableName","start_node_val","node_pk_field","target_node_op",
                                                "target_node_field_value", "target_node_field","target_node_join_field"};
        Type[] td_field_types = new Type[] {Type.STRING_TYPE, Type.STRING_TYPE, Type.STRING_TYPE, Type.STRING_TYPE, Type.INT_TYPE, Type.STRING_TYPE, 
                                            Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE};

        TupleDesc mv_td = new TupleDesc(td_field_types, td_field_names);

        try {
            writeMaterializedTable();
            createMaterializedDFSTable(mv_td, mv_dfsjoin_table_name);
    
            Tuple mv_t = new Tuple(mv_td);
            // This is just setting what the materialized view table name should be
            mv_t.setField(0, new StringField(table_name, 128));
            mv_t.setField(1, new StringField(node_table_name, 64));
            mv_t.setField(2, new StringField(edge_table_name, 64));
            mv_t.setField(3, new StringField(start_node_val.toString(), 128));
            mv_t.setField(4, new IntField(node_pk_field));
            mv_t.setField(5, new StringField(Parser.getOpString(target_node_op),  64));
            mv_t.setField(6, new StringField(target_node_field_value.toString(), 64));
            mv_t.setField(7, new IntField(target_node_field));
            mv_t.setField(8, new IntField(target_node_join_field));

            int mv_tableid = Database.getCatalog().getTableId(mv_dfsjoin_table_name);

            bp.insertTuple(tid, mv_tableid, mv_t);
            bp.transactionComplete(tid, true);
            } catch(IOException e) {
                System.out.println("Brutal Failure");
            } catch(DbException e) {
    
            } catch(TransactionAbortedException e) {
    
            }
        return;
    }

    private void writeMaterializedTable() throws DbException, TransactionAbortedException, IOException {
        hf = Utility.createEmptyHeapFile(table_name, this.td);
        Database.getCatalog().addTable(hf, table_name);
        System.out.println("Table Name from Id: " + Database.getCatalog().getTableId(table_name));
        BufferPool bp = Database.getBufferPool();
        query_result.open();
        while(query_result.hasNext()) {
            Tuple next_tuple_result = query_result.next();
            System.out.println("Inserting tuple " + next_tuple_result + " To File");
            bp.insertTuple(tid, hf.getId(), next_tuple_result);
        }
    }

    public void materializeSubPaths(String node_name, String edge_name, Field start_node_val,
                                    Field target_node_val, int val_field, Predicate.Op target_op,
                                    Predicate.Op start_op
    ){
        try {
            writeMaterializedTable();
            Tuple subpath_tuple = createSubPathTuple(table_name, node_name, edge_name, start_node_val, target_node_val, val_field, target_op, start_op);
            createMaterializedDFSTable(subpath_tuple.getTupleDesc(), mv_subpaths_table_name);
            // Now we need to add that tuple
            int mv_tableid = Database.getCatalog().getTableId(mv_subpaths_table_name);
            bp.insertTuple(tid, mv_tableid, subpath_tuple);
            bp.transactionComplete(tid, true);            
        } catch(IOException e) {
            e.printStackTrace();
            System.out.println("Brutal Failure");
        } catch(DbException e) {
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            e.printStackTrace();
        }
    }


    public SeqScan getMaterializedSubPaths(String node_name, String edge_name, Field start_node_val,
                                              Field target_node_val, int val_field, Predicate.Op target_op,
                                              Predicate.Op start_op
    ){
        try {
            boolean mv_table_exists = Database.getCatalog().tableExists(mv_subpaths_table_name);
            if (!mv_table_exists) { 
                return null;
            }
            int file_id = Database.getCatalog().getTableId(mv_subpaths_table_name);
            HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(file_id);
            Tuple expected_tuple = createSubPathTuple("placeholder", node_name, edge_name, start_node_val, target_node_val, val_field, target_op, start_op);
            SeqScan ss = new SeqScan(tid, file_id, "");
            ss.open();
            while (ss.hasNext()) {
                Tuple t = ss.next();
                boolean tuple_matches = true;
                for (int i=1;i<t.getTupleDesc().numFields();i++) {
                    if (!t.getField(i).equals(expected_tuple.getField(i))) {
                        tuple_matches = false;
                    }
                } if (tuple_matches) {
                    // Do something cool
                    // like return with the table name or OpIterator or something
                    int mv_table_id = Database.getCatalog().getTableId(t.getField(0).toString());
                    return new SeqScan(tid, mv_table_id, "");
                }
                
            }
            return null;
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    private Tuple createSubPathTuple(String table_name, String node_name, String edge_name, Field start_node_val,
                                        Field target_node_val, int val_field, Predicate.Op target_op,
                                        Predicate.Op start_op
    ) throws IOException {

        Type[] td_typeAr = new Type[] {Type.STRING_TYPE, Type.STRING_TYPE, Type.STRING_TYPE, Type.STRING_TYPE,
                                        Type.STRING_TYPE, Type.INT_TYPE, Type.STRING_TYPE, Type.STRING_TYPE};
        String[] td_fieldAr = new String[] {"TableName", "NodeTableName","EdgeTableName","StartNodeVal",
                                            "TargetNodeVal","ValField","TargetOp","StartOp"};
        TupleDesc td = new TupleDesc(td_typeAr, td_fieldAr);
        Tuple t = new Tuple(td);

        t.setField(0, new StringField(table_name));
        t.setField(1, new StringField(node_name));
        t.setField(2, new StringField(edge_name));
        t.setField(3, new StringField(start_node_val.toString()));
        t.setField(4, new StringField(target_node_val.toString()));
        t.setField(5, new IntField(val_field));
        t.setField(6, new StringField(Parser.getOpString(target_op)));
        t.setField(7, new StringField(Parser.getOpString(start_op)));

        return t;
    }

    private void createMaterializedDFSTable(TupleDesc mv_td, String table_name) throws IOException {
        boolean mv_table_exists = Database.getCatalog().tableExists(table_name);
        if (!mv_table_exists) {
            System.out.println("Creating new materialized view record table for table: " + table_name);
            HeapFile mv_hf = Utility.createEmptyHeapFile(table_name, mv_td);
            Database.getCatalog().addTable(mv_hf, table_name);
            System.out.println("Table name: " + table_name + " ID: " + Database.getCatalog().getTableId(table_name));
        }
    }

}