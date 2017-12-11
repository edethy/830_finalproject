package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
	
    int ioCostPerPage;
    int num_pages;
    int num_tups = 0;
    int tableid;
    
    ConcurrentHashMap<Integer, StringHistogram> string_field_hist_map = new ConcurrentHashMap<Integer, StringHistogram>();
    ConcurrentHashMap<Integer, IntHistogram> int_field_hist_map = new ConcurrentHashMap<Integer, IntHistogram>();
    ConcurrentHashMap<Integer, int[]> field_minmax_map = new ConcurrentHashMap<Integer, int[]>();
    ArrayList<Integer> int_fields = new ArrayList<Integer>();

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
    	this.tableid = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    	SeqScan file_scan = new SeqScan(new TransactionId(), tableid, "");
    	num_pages = ((HeapFile)(Database.getCatalog().getDatabaseFile(tableid))).numPages();
    	TupleDesc td = file_scan.getTupleDesc();    	
    	Iterator<TupleDesc.TDItem> td_iterator = td.iterator();
    	int j=0;
    	while (td_iterator.hasNext()) {
            TupleDesc.TDItem td_item = td_iterator.next();
            System.out.println("tablestat j:" + j);
    		if (td_item.fieldType == Type.INT_TYPE) {
    			int_fields.add(j);
    		} else {
    			string_field_hist_map.put(j, new StringHistogram(NUM_HIST_BINS));
    		}
    		j++;
    	}
    	try {
        	file_scan.open();
        	while (file_scan.hasNext()) {
        		Tuple next_tup = file_scan.next();
        		num_tups++;
        		for (int i=0;i<int_fields.size();i++) {
        			int[] minmax_map;
        			int f_index = int_fields.get(i);
        			int f_value = ((IntField)next_tup.getField(f_index)).getValue();
        			minmax_map = field_minmax_map.get(f_index);
        			
        			if (minmax_map != null && minmax_map[0] > f_value) {
        				minmax_map[0] = f_value;
        			} if (minmax_map != null && minmax_map[1] < f_value) {
        				minmax_map[1] = f_value;
        			} else if (minmax_map == null) {
        				minmax_map = new int[] {f_value, f_value};
        			}
        			field_minmax_map.put(f_index, minmax_map);
        		}
        	}
        	for (int i=0;i<int_fields.size();i++) {
                int[] minmax_map = field_minmax_map.get(int_fields.get(i));
                if (minmax_map[1]-minmax_map[0] == 0) 
                int_field_hist_map.put(int_fields.get(i), new IntHistogram(1, minmax_map[0], minmax_map[1]));
                else int_field_hist_map.put(int_fields.get(i), new IntHistogram(Math.min(NUM_HIST_BINS, minmax_map[1]-minmax_map[0]), minmax_map[0], minmax_map[1]));
        	}
    		file_scan.rewind();
    		while (file_scan.hasNext()) {    			
                Tuple next_tup = file_scan.next();
                System.out.println("num field:" + next_tup.getTupleDesc().numFields());
    	    	for(int z=0;z<next_tup.getTupleDesc().numFields();z++) {
    				if (string_field_hist_map.containsKey(z)) {
    					StringHistogram st = string_field_hist_map.get(z);
    					String tuple_val = ((StringField)next_tup.getField(z)).getValue();
    					st.addValue(tuple_val);
    				} else {
    					IntHistogram it = int_field_hist_map.get(z);
    					int tuple_val = ((IntField)next_tup.getField(z)).getValue();
    					it.addValue(tuple_val);
    				}
    			}
    		}
    	} catch (Exception e) {e.printStackTrace(System.out);}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return num_pages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        double table_card = num_tups*selectivityFactor;
        return (int)table_card;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if (constant.getType() == Type.STRING_TYPE) {
    		String field_val = ((StringField)constant).getValue();
    		StringHistogram st = string_field_hist_map.get(field);
    		return st.estimateSelectivity(op, field_val);
    	} else {
    		int field_val = ((IntField)constant).getValue();
    		IntHistogram it = int_field_hist_map.get(field);
    		return it.estimateSelectivity(op, field_val);
    	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.num_tups;
    }

}
