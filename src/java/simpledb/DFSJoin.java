package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class DFSJoin extends Operator {

	private Field start_node_value;

    private int start_node_field = 0;
    private int target_node_field_index = 1;

    private int node_pk_field;
    
    private int target_node_field;
    private Field target_node_field_value;

    private int target_node_join_field;
    
    private Predicate.Op target_node_op;

	private OpIterator edges;
    private OpIterator nodes;
	
	private HashSet<Field> nodes_visited;
		
    private static final long serialVersionUID = 1L;

    private TupleDesc td;
    private ArrayList<Tuple> tuple_path_list = new ArrayList<Tuple>();
    private Iterator<Tuple> tuple_iterator;

    public DFSJoin(OpIterator nodes, OpIterator edges, Field start_node_value, 
    int node_pk_field, Predicate.Op target_node_op, Field target_node_field_value, int target_node_field, int target_node_join_field) {
        this.nodes = nodes;
        this.edges = edges;
        this.start_node_value = start_node_value;
        nodes_visited = new HashSet<Field>();
        this.node_pk_field = node_pk_field;

        this.target_node_op = target_node_op;
        this.target_node_field = target_node_field;
        this.target_node_field_value = target_node_field_value;
        this.target_node_join_field = target_node_join_field;

        TupleDesc edges_td = edges.getTupleDesc();
        Type node_key_type = edges_td.getFieldType(target_node_join_field);
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE, node_key_type}, new String[] {"path_number", "path_index", "node"});
        generateTuplePaths();
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    private void generateTuplePaths() {
        HashSet<ArrayList<Field>> paths = getPaths();
        System.out.println("Tuple Paths in Constructor " + paths);
        int path_index = 0;
        for (ArrayList<Field> p : paths) {
            for (int i=0; i<p.size(); i++) {
                Tuple t = new Tuple(td);
                t.setField(0, new IntField(path_index));
                t.setField(1, new IntField(i));
                t.setField(2, p.get(i));
                tuple_path_list.add(t);
            }
            path_index++;
        }
        nodes_visited = new HashSet<Field>();
        nodes.close();
        edges.close();	
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

    public HashSet<ArrayList<Field>> getPaths() {
    	try {
        	return getPaths(this.start_node_value, new ArrayList<Field>(), this.edges, this.nodes);	
    	} catch(DbException e) {
    		e.printStackTrace();
    	} catch (TransactionAbortedException e) {
    		e.printStackTrace();
    	}
        HashSet<ArrayList<Field>> f = new HashSet<ArrayList<Field>>();
    	return f;
    }

    @SuppressWarnings("unchecked")
    private HashSet<ArrayList<Field>> getPaths(Field s_node, ArrayList<Field> path_so_far, OpIterator edges, OpIterator nodes) 
    throws DbException, TransactionAbortedException {
        edges.rewind();
        nodes.rewind();
        System.out.println("Getting Paths");
    	HashSet<ArrayList<Field>> paths_from_node = new HashSet<ArrayList<Field>>();
    	path_so_far.add(s_node);
    	nodes_visited.add(s_node);
    	Predicate p = new Predicate(this.start_node_field, Predicate.Op.EQUALS, s_node);
    	Filter edges_to_follow = new Filter(p, edges);

    	edges_to_follow.open();
    	System.out.println("Edges to follow" + edges_to_follow);
        
    	while(edges_to_follow.hasNext()) {
    		ArrayList<Field> path = (ArrayList<Field>)path_so_far.clone();
    		Tuple next_edge = edges_to_follow.next();
    		System.out.println("Next Edge " + next_edge);

            System.out.println("Target node join field " + target_node_join_field);
    		Field next_node_id_field = next_edge.getField(target_node_join_field);
            System.out.println("Next node id field " + next_node_id_field);
            Predicate p2 = new Predicate(node_pk_field, Predicate.Op.EQUALS, next_node_id_field);
            Filter get_next_node_iterator = new Filter(p2, nodes);
            get_next_node_iterator.open();
            boolean end_node_reached = false;
            while (get_next_node_iterator.hasNext()) {
                System.out.println("This filter should only run once here");
                Tuple next_node = get_next_node_iterator.next();
                System.out.println("Next Node " + next_node);
                Field filter_field = next_node.getField(this.target_node_field);
                end_node_reached = filter_field.compare(this.target_node_op, this.target_node_field_value);
            }
            get_next_node_iterator.close();
            System.out.println("End Node Reached "  + end_node_reached);
            if (end_node_reached) {
                path.add(next_node_id_field);
                paths_from_node.add(path);
            } else if (nodes_visited.contains(next_node_id_field)) {
                System.out.println("Next Node Id Field " + next_node_id_field + " Nodes visited " + nodes_visited);
                continue;
            } else {
                System.out.println("Recursing on subpaths");
                HashSet<ArrayList<Field>> subpaths = getPaths(next_node_id_field, path, edges, nodes);
    			paths_from_node.addAll(subpaths);
            }
    	}
        // edges_to_follow.close();
        System.out.println("Return nodes: " + paths_from_node);
    	return paths_from_node;
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
