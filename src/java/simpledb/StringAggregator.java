package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

	private int gbfield;
	private Type gbfieldtype;
	private boolean groupby = false;
	private int afield;
	private Op agg_op;
	// count_agg_grouping only used when groupby
	private Hashtable<Field, Integer> count_agg_grouping;
	private int total_count;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (what != Aggregator.Op.COUNT) { throw new IllegalArgumentException("Only COUNT Aggregator accepted"); }
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.agg_op = what;
    	this.total_count = 0;
    	this.count_agg_grouping = new Hashtable<Field, Integer>();
    	
    	if (gbfield != NO_GROUPING) {
    		groupby = true;
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if (!groupby) {
    		total_count++;
    	} else {
    		int current_count;
    		if (count_agg_grouping.containsKey(tup.getField(gbfield))) {
    			current_count = count_agg_grouping.get(tup.getField(gbfield));
    			current_count++;
    		} else {
    			current_count = 1;
    		}
			count_agg_grouping.put(tup.getField(gbfield), current_count);
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	return new TupleIterator(get_td(), get_tuple_list());
    }

    private TupleDesc get_td() {
    	if (!groupby) {
    		return new TupleDesc(new Type[]{Type.INT_TYPE});
    	}
    	return new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    }
    
    private ArrayList<Tuple> get_tuple_list() {
    	TupleDesc td;
    	Tuple t;
    	ArrayList<Tuple> t_list = new ArrayList<Tuple>();
    	if (!groupby) {
    		td = get_td();
    		t = new Tuple(td);
    		t.setField(0, new IntField(total_count));
    		t_list.add(t);
    	} else {
    		for (Field f: count_agg_grouping.keySet()) {
    			td = get_td();
    			t = new Tuple(td);
    			t.setField(0, f);
    			t.setField(1, new IntField(count_agg_grouping.get(f)));
    			t_list.add(t);
    		}
     	}
    	return t_list;
    }
    
}
