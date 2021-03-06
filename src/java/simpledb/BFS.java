package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class BFS extends Operator {

	private Field start_node;
	private Field end_node;
	
	private OpIterator edges;
	private int start_node_field;
	private int target_node_field;
	
	private Filter edge_to_follow;
	
	private HashSet<Field> nodes_visited;
	
    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts start node,  iterator of edges, and predicate to determine
     * when to accept a node as final
     * 
     * @param p
     *            The predicate to use to join the children
     * @param start_node
     *            Tuple for first node in path
     * @param edges
     *            Result of join between nodes and edges table to give flat structure
     */
    public BFS(Field start_node_id, Field end_node_id, OpIterator edges, int start_node_field, int target_node_field) {
    	this.start_node = start_node_id;
    	this.end_node = end_node_id;
    	this.edges = edges;
    	this.start_node_field = start_node_field;
    	this.target_node_field = target_node_field;
    	Predicate p = new Predicate(start_node_field, Predicate.Op.EQUALS,start_node_id);
    	this.edge_to_follow = new Filter(p, edges);
    	nodes_visited = new HashSet<Field>();
    }
 
   
    public HashSet<ArrayList<Field>> getPaths() {
    	try {
        	return getPaths(this.start_node, this.end_node, new ArrayList<Field>(), this.edges);	
    	} catch(DbException e) {
    		e.printStackTrace();
    	} catch (TransactionAbortedException e) {
    		e.printStackTrace();
    	}
    	return null;
    }
       
    @SuppressWarnings("unchecked")
    private HashSet<ArrayList<Field>> getPaths(Field s_node, Field e_node, ArrayList<Field> path_so_far, OpIterator edges) 
    throws DbException, TransactionAbortedException {
        edges.rewind();
    	HashSet<ArrayList<Field>> paths_from_node = new HashSet<ArrayList<Field>>();
    	path_so_far.add(s_node);
    	nodes_visited.add(s_node);
    	Predicate p = new Predicate(this.start_node_field, Predicate.Op.EQUALS, s_node);
    	Filter edges_to_follow = new Filter(p, edges);
    	edges_to_follow.open();
    	// System.out.println("Edges to follow" + edges_to_follow);
    	while(edges_to_follow.hasNext()) {
    		ArrayList<Field> path = (ArrayList<Field>)path_so_far.clone();
    		Tuple next_edge = edges_to_follow.next();
    		System.out.println("Next Edge " + next_edge);
            Field next_node = next_edge.getField(target_node_field);
            if (e_node.equals(next_node)) {
            //    System.out.println("We found an end");
                path.add(e_node);
                paths_from_node.add(path);
            }
    		else if (next_node == s_node) {
    			continue;
    		} else if (nodes_visited.contains(next_node)) {
    			continue;
    		} else if (e_node.equals(next_node)) {
    			path.add(e_node);
    			paths_from_node.add(path);
    		} else {
    			HashSet<ArrayList<Field>> subpaths = getPaths(next_node, e_node, path, edges);
    			paths_from_node.addAll(subpaths);
    		}
    	}
    	return paths_from_node;    	
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
    	return "";
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
    	return "";
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
    	return edges.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
    }

    public void close() {
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	return null;
    }

    @Override
    public OpIterator[] getChildren() {
    	return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	
    }
    

}
