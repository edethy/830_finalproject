package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class SubPaths extends Operator {
    
	private OpIterator edges;
    private OpIterator nodes;
			
    private static final long serialVersionUID = 1L;

    private ArrayList<Tuple> tuple_path_list = new ArrayList<Tuple>();
    private Iterator<Tuple> tuple_iterator;

    // SubPaths take the form: PathNumber, PathIndex, StartNode (int), EndNode (int)
    private TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE},
                                         new String[] {"PathNumber", "PathIndex", "Node", "PageNumber"});
    private int node_pk_field = 0;
    private int start_node_field_index = 0;
    private int target_node_field_index = 1;

    private Field start_node_value;
    private Field target_node_value;
    private int value_field;

    private Predicate.Op target_node_op;
    private Predicate.Op start_node_op;

    private int last_pgno = 0;
    private int last_tupno = 0;
    private int latest_path_index = 0;

    private int start_tup_no;
    private int start_pg_no;

    private HashMap<Field,Tuple> node_tuples = new HashMap<Field, Tuple>();

    private int last_pending_pgno = 0;
    private int last_pending_tupno = 0;

    private int path_index = 0;


    public SubPaths(OpIterator nodes, OpIterator edges, Field start_node_value, 
                    Field target_node_value, int value_field,
                    Predicate.Op target_node_op, Predicate.Op start_node_op
    ){
        this.nodes = nodes;
        this.edges = edges;
        this.start_node_value = start_node_value;
        this.target_node_value = target_node_value;
        this.value_field = value_field;
        this.target_node_op = target_node_op;
        this.start_node_op = start_node_op;
        load_nodes_into_mem();        
        this.tuple_path_list =  createTupleList();
        this.start_pg_no = 0;
        this.start_tup_no = 0;
    }

    public SubPaths(OpIterator nodes, OpIterator edges, Field start_node_value, 
                    Field target_node_value, int value_field,
                    Predicate.Op target_node_op, Predicate.Op start_node_op,
                    int start_tup_no, int start_pg_no
    ){
        // System.out.println("Correct Constructor");
        this.nodes = nodes;
        this.edges = edges;
        this.start_node_value = start_node_value;
        this.target_node_value = target_node_value;
        this.value_field = value_field;
        this.target_node_op = target_node_op;
        this.start_node_op = start_node_op;
        this.start_tup_no = start_tup_no;
        this.start_pg_no = start_pg_no;
        load_nodes_into_mem();        
        this.tuple_path_list =  createTupleList();

        // System.out.println("Consturment: " + start_tup_no + " Pg NO: " + start_pg_no);
    }

    public int getLastPageNumber() {
        return last_pgno;
    }
    public int getLastTupleNumber() {
        return last_tupno;
    }
    public int getLatestPathIndex() {
        return path_index;
    }

    /**ublic int 
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }
    
    private void load_nodes_into_mem() {
        try {
            nodes.rewind();
            while (nodes.hasNext()) {
                Tuple n = nodes.next();
                node_tuples.put(n.getField(node_pk_field), n);
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    private ArrayList<Tuple> createTupleList() {
        try {
            HashSet<ArrayList<Tuple>> paths = generateSubPaths();
            ArrayList<Tuple> set_tuple_paths = new ArrayList<Tuple>();
            for (ArrayList<Tuple> p : paths) {
                for (int i=0; i<p.size(); i++) {
                    set_tuple_paths.add(p.get(i));
                }
            }
            // System.out.println("Tuple List with Materialization:" + set_tuple_paths);
            return set_tuple_paths;
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        return new ArrayList<Tuple>();
    }

    private HashSet<ArrayList<Tuple>> generateSubPaths() throws DbException, TransactionAbortedException {
        HashSet<Field>  pending_start_nodes = new HashSet<Field>();
        HashMap<Field, ArrayList<Tuple>> pending_paths = new HashMap<Field,ArrayList<Tuple>>();
        HashSet<ArrayList<Tuple>> paths = new HashSet<ArrayList<Tuple>>();
        edges.open();
        nodes.open();
        Tuple edge = null;
        while (edges.hasNext()) {
            edge = edges.next();
            // System.out.println("Edge Pg Number: " + edge.getRecordId().getPageId().getPageNumber());
            // System.out.println("Edge Tup Number; " + edge.getRecordId().getTupleNumber());
            // System.out.println("Start tup number; " + start_tup_no);
            // System.out.println("Start pg number: " + start_pg_no);
            if (!(start_pg_no == 0 && start_tup_no == 0)) {
                if (edge.getRecordId().getTupleNumber() < start_tup_no && (edge.getRecordId().getPageId().getPageNumber() == start_pg_no)) {
                    // System.out.println("Ignoring Edge");
                    continue;
                }
            }

            Field start_node = edge.getField(start_node_field_index);

            if (start_node.equals(new IntField(-1))) {
                pending_paths.clear();
                pending_start_nodes.clear();
                continue;
            }
            Tuple node = node_tuples.get(start_node);
            Field node_field = node.getField(value_field);

            for (ArrayList<Tuple> p : pending_paths.values()) {
                p.add(node);
                if (node_field.compare(target_node_op, target_node_value)) {
                    ArrayList<Tuple> path_tuple = createPathTupleList(p);
                    paths.add(path_tuple);
                }
            }
            if (pending_start_nodes.contains(start_node) || node_field.compare(start_node_op, start_node_value)) {
                ArrayList<Tuple> new_path = new ArrayList<Tuple>();
                new_path.add(node);
                pending_paths.put(start_node, new_path);
                pending_start_nodes.add(start_node);
            } else if (node_field.compare(target_node_op, target_node_value)) {           
                pending_paths.clear();
                pending_start_nodes.clear();
            }
        }
        if (edge != null) {
            Field last_node_key = edge.getField(target_node_field_index);
            Tuple last_node = node_tuples.get(last_node_key);
            Field node_field = last_node.getField(value_field);
            if (node_field.compare(target_node_op, target_node_value)) {
                for (ArrayList<Tuple> p : pending_paths.values()) {
                    p.add(last_node);
                    ArrayList<Tuple> path_tuple = createPathTupleList(p);
                    paths.add(path_tuple);
                }
            }
            int edge_pgno = edge.getRecordId().getPageId().getPageNumber();
            last_pgno = edge_pgno;
            last_tupno = edge.getRecordId().getTupleNumber();
            System.out.println("Num Paths: " + path_index);
        }

        int last_page = 0;
        int ll = 0;
        for (Field f : pending_start_nodes) {
            Tuple pending_tup = node_tuples.get(f);
            if (pending_tup.getRecordId().getPageId().getPageNumber() >= last_page && pending_tup.getRecordId().getTupleNumber() >= last_tupno) {
                last_page = pending_tup.getRecordId().getPageId().getPageNumber();
                ll = pending_tup.getRecordId().getTupleNumber();
            }
        }
        last_pending_pgno = last_page;
        last_pending_tupno = ll;
        return paths;
    }

    private ArrayList<Tuple> createPathTupleList(ArrayList<Tuple> p) {
        ArrayList<Tuple> new_tuple = new ArrayList<Tuple>();
        for (int i=0;i<p.size();i++) {
            // System.out.println("Adding tuple with record id: " + p.get(i).getRecordId().getTupleNumber());
            // System.out.println("Tuple has Page number: " + p.get(i).getRecordId().getPageId().getPageNumber());
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(path_index));
            t.setField(1, new IntField(i));
            t.setField(2, p.get(i).getField(node_pk_field));
            t.setField(3, new IntField(0));
            new_tuple.add(t);
        }
        path_index++;
        return new_tuple;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        tuple_iterator = tuple_path_list.iterator();
    }

    public void close() {
        tuple_iterator = null;
        super.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();
    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (tuple_iterator.hasNext()) {
            Tuple t = tuple_iterator.next();
            return t;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
    	return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        return;
    }
}
