package simpledb;

public class MaterializeViewUtil {

    public String mv_subpaths_table_name = "mv_subpaths";

    // We have a TupleDesc that describes the Materialized View

    public TupleDesc getSubPathMVTD() {
        Type[] td_typeAr = new Type[] {Type.STRING_TYPE, Type.STRING_TYPE, Type.INT_TYPE,
                                       Type.INT_TYPE, Type.INT_TYPE};
        String[] td_fieldAr = new String[] {"TableName", "Query", "PageNumber","TupleNumber","NumPaths"};
        TupleDesc td = new TupleDesc(td_typeAr, td_fieldAr);
        return td;
    
    }

    public Tuple createSubPathTuple(String table_name, String query, int pgno, int tupleno, int numpaths) {

        TupleDesc td = getSubPathMVTD();
        Tuple t = new Tuple(td);

        t.setField(0, new StringField(table_name));
        t.setField(1, new StringField(query));
        t.setField(2, new IntField(pgno));
        t.setField(3, new IntField(tupleno));
        t.setField(4, new IntField(numpaths));

        return t;
    }


    // public SeqScan getMaterializedSubPaths(TransactionId tid, String query) ){
    //     try {
    //         boolean mv_table_exists = Database.getCatalog().tableExists(mv_subpaths_table_name);
    //         if (!mv_table_exists) { 
    //             return null;
    //         }

    //         int file_id = Database.getCatalog().getTableId(mv_subpaths_table_name);
    //         HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(file_id);
    //         Tuple expected_tuple = createSubPathTuple("", node_name, edge_name, start_node_val, target_node_val, val_field, target_op, start_op);
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