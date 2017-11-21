package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

	private int gbfield; // group-by field
	private Type gbfieldtype; // group-by field type
	private int afield; // aggregate field
	private Op ag_op; // aggregation operator
	private int total_agg;
	private int total_tups;
	private Hashtable<Field, Integer> avg_count_byfield;
	private Hashtable<Field, Integer> agg_by_field;
	private boolean groupby;
	private int total_sum;
    
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.ag_op = what;
    	this.avg_count_byfield = new Hashtable<Field, Integer>();
    	this.agg_by_field = new Hashtable<Field, Integer>();
    	this.groupby = (gbfield != NO_GROUPING);
    	this.total_tups = 0;
    	this.total_sum = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	int field_value = ((IntField)tup.getField(afield)).getValue();
		total_sum += field_value;
    	int new_val;
    	if (!groupby) {
    		
    		if (total_tups == 0) {
    			int init_val = init_tup_agg(field_value);
    			total_agg = init_val;
    		} else {
        		new_val = merge_by_op(field_value,total_agg, total_tups);
        		total_agg = new_val;
    		}
    	} else {
    		Field gb_field = tup.getField(gbfield);
    		if (agg_by_field.containsKey(gb_field)){
				new_val = merge_by_op(field_value, agg_by_field.get(tup.getField(gbfield)), avg_count_byfield.get(tup.getField(gbfield)));
				agg_by_field.put(tup.getField(gbfield), new_val);
				int old_count = avg_count_byfield.get(tup.getField(gbfield));
				int new_count = old_count + 1;
				avg_count_byfield.put(tup.getField(gbfield), new_count);
    		} else {
    			int init_val = init_tup_agg(field_value);
    			agg_by_field.put(tup.getField(gbfield), init_val);
    			avg_count_byfield.put(tup.getField(gbfield), 1); 
    		}
    	}
    	total_tups++;
    }

    private int init_tup_agg(int field_value) {
    	if (ag_op != Aggregator.Op.COUNT) {
    		total_agg = field_value;
    	} else {
    		total_agg = 1;
    	}
    	return total_agg;
    }
    private int merge_by_op(int new_value, int cur_value, int avg_count) {
    	switch(ag_op) {
    	case MIN:
    		if (new_value < cur_value) { return new_value; }
    		else { return cur_value; }
    	case MAX:
    		if (new_value > cur_value) { return new_value; }
    		else { return cur_value; }
    	case SUM:
    		return cur_value + new_value;
    	case COUNT:
    		return cur_value + 1;
    	case AVG:
    		return cur_value + new_value;
    	}
    	return 0;
    }
    
    
    private int get_avg(Field f) {
    	int total_sum = agg_by_field.get(f);
    	int total_tups = avg_count_byfield.get(f);
    	return total_sum/total_tups;
    }
    
    private int get_avg() {
    	return total_agg/total_tups;
    }
    
    private TupleDesc get_td() {
    	if(groupby) {
    		return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    	}
    	return new TupleDesc(new Type[]{Type.INT_TYPE});
    }
    
    private ArrayList<Tuple> get_tuple_list() {
    	TupleDesc td = get_td();
    	Tuple t;
    	ArrayList<Tuple> t_list = new ArrayList<Tuple>();
    	int agg;
    	if (!groupby) {
    		t = new Tuple(td);
    		if (ag_op == Aggregator.Op.AVG) { agg = total_agg/total_tups; }
    		else { agg = total_agg; }
    		t.setField(0, new IntField(agg));
    		t_list.add(t);
    	} else {
    		for (Field f: agg_by_field.keySet()) {
    			t = new Tuple(td);
    			t.setField(0, f);
    			if (ag_op == Aggregator.Op.AVG) {  agg = agg_by_field.get(f)/avg_count_byfield.get(f); }
    			else { agg = agg_by_field.get(f); }
    			t.setField(1, new IntField(agg));
    			t_list.add(t);
    		}
     	}
    	return t_list;
    }
    
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
    	TupleIterator tuple_it = new TupleIterator(get_td(), get_tuple_list());
    	return tuple_it;
    }

}
