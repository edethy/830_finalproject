package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int min;
	private int max;
	private int buckets;
	private int[] hist;
	private double bucket_size;
	private int total_tups = 0;
	
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.bucket_size = (max - min)/((double)buckets);
    	this.hist = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int bucket;
    	if (v==min) {
    		bucket = 0;
    	} else if (v==max) {
    		bucket = this.buckets-1;
    	} else {
    		bucket = (int)((v-min)/this.bucket_size);
    	}
    	hist[bucket] += 1;
    	total_tups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    //	System.out.println("Histogram: " + toString());
    	int bucket = getBucket(v);
    	double selectivity;
    	switch(op) {
    	case EQUALS:
    		// if value not in range, nothing in table equal
    		if (bucket == -1 || bucket == this.buckets) { return 0.0; }
    		return ((hist[bucket]/bucket_size)/total_tups);
    	case NOT_EQUALS:
    		// if value not in range, nothing in table is equal
    		return (1 - estimateSelectivity(op.EQUALS, v));
    	case GREATER_THAN:
    		if (bucket == -1) { return 1.0; }
    		if (bucket == this.buckets) { return 0; }
    		selectivity = ((hist[bucket])/total_tups) * ((min + bucket_size * (bucket+1) - v)/bucket_size);
    		for (int i=bucket+1;i<buckets;i++) {
    			selectivity += (hist[i]/(double)total_tups);
    		}
    		return selectivity;
    	case LESS_THAN:
    		if (bucket == -1) { return 0.0; }
    		if (bucket == this.buckets) { return 1.0; }
    		selectivity = ((hist[bucket])/total_tups) * ((v - min + bucket_size * (bucket))/bucket_size);
    		for (int i=0;i<bucket;i++) {
    			selectivity += (hist[i]/(double)total_tups);
    		}
    		return selectivity;
    	case GREATER_THAN_OR_EQ:
    		// selectivity of equals + selectivity of greater.
    		selectivity = estimateSelectivity(op.EQUALS, v) + estimateSelectivity(op.GREATER_THAN, v);
    		return selectivity;
    	case LESS_THAN_OR_EQ:
    		return estimateSelectivity(op.EQUALS, v) + estimateSelectivity(op.LESS_THAN, v);
    	}
        return -1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
    	String start_string = "Histogram Values in Ranges: \n";
    	for (int i=0; i<buckets;i++) {
    		double min_bucket_val = min + bucket_size * i;
    		double max_bucket_val = min + bucket_size * (i + 1);
    		int num_in_bucket = hist[i];
    		start_string = start_string + min_bucket_val + " - " + max_bucket_val + " : " + num_in_bucket + " \n";
    	}
        return start_string;
    }
    
    
    private int getBucket(int v) {
    	int bucket;
    	if (v==min) {
    		bucket = 0;
    	} else if (v==max) {
    		bucket = this.buckets-1;
    	} else if (v < min) {    	
    		return -1;
    	} else if (v> max) {
    		return this.buckets;
    	} else {
    		//bucket = Math.floor(((double)(v-min))/this.bucket_size);
    		bucket = (int)((v-min)/this.bucket_size);
    	}
    	return bucket;
    }
}
