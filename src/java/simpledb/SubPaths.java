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

    private HashMap<Field,Tuple> node_tuples = new HashMap<Field, Tuple>();

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
        this.tuple_path_list =  createTupleList();
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }
    
    private void load_nodes_into_mem() {
        try {
            nodes.open();
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
            int path_index = 0;
            for (ArrayList<Tuple> p : paths) {
                for (int i=0; i<p.size(); i++) {
                    Tuple t = new Tuple(td);
                    t.setField(0, new IntField(path_index));
                    t.setField(1, new IntField(i));
                    t.setField(2, p.get(i).getField(node_pk_field));
                    set_tuple_paths.add(t);
                }
                path_index++;
            }
            return set_tuple_paths;
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
        return new ArrayList<Tuple>();
    }

    private HashSet<ArrayList<Tuple>> generateSubPaths() throws DbException, TransactionAbortedException {
        load_nodes_into_mem();
        // System.out.println("Node Tuples in Memory:\n" + node_tuples);
        HashSet<Field>  pending_start_nodes = new HashSet<Field>();
        HashMap<Field, ArrayList<Tuple>> pending_paths = new HashMap<Field,ArrayList<Tuple>>();
        HashSet<ArrayList<Tuple>> paths = new HashSet<ArrayList<Tuple>>();
        edges.open();
        nodes.open();
        Tuple edge = null;
        while (edges.hasNext()) {
            edge = edges.next();
            Field start_node = edge.getField(start_node_field_index);

            if (start_node.equals(new IntField(-1))) {
                pending_paths.clear();
                pending_start_nodes.clear();
                continue;
            }
            Tuple node = node_tuples.get(start_node);
            Field node_field = node.getField(value_field);
            // System.out.println("Edge: " + edge + 
            //                     "\nStart node: " + start_node +
            //                     "\nNode: " + node + 
            //                     "\nNodeField: " + node_field                
            // );

            for (ArrayList<Tuple> p : pending_paths.values()) {
                p.add(node);
                if (node_field.compare(target_node_op, target_node_value)) {
                    System.out.println("We've reached a target node so adding to path");
                    paths.add(p);
                }
            }
            if (pending_start_nodes.contains(start_node) || node_field.compare(start_node_op, start_node_value)) {
                System.out.println("Starting new path for node: " + start_node);
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
            // So we had a last edge we need to consider target node
            Field last_node_key = edge.getField(target_node_field_index);
            Tuple last_node = node_tuples.get(last_node_key);
            Field node_field = last_node.getField(value_field);
            // Check if matches end of anything
            if (node_field.compare(target_node_op, target_node_value)) {
                for (ArrayList<Tuple> p : pending_paths.values()) {
                    p.add(last_node);
                    paths.add(p);
                }
            }
        }
        System.out.println("Paths: " + paths);
        return paths;
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
