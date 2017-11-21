package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> td_itemAr = new ArrayList<TDItem>();
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;
        
        public final Integer index;

        public TDItem(Type t, String n, Integer index) {
            this.fieldName = n;
            this.fieldType = t;
            this.index = index;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	return td_itemAr.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	for (int i=0; i<typeAr.length;i++) {
    		TDItem new_item = new TDItem(typeAr[i],fieldAr[i], i);
    		td_itemAr.add(new_item);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	for (int i=0;i<typeAr.length;i++) {
    		TDItem new_item = new TDItem(typeAr[i],null, i);
    		td_itemAr.add(new_item);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return td_itemAr.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i >= td_itemAr.size()){
    		throw new NoSuchElementException("Invalid Field Reference");
    	}
    	return td_itemAr.get(i).fieldName;    
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {    	
    	if (i < td_itemAr.size()) {
    		return td_itemAr.get(i).fieldType;
    	}
        throw new NoSuchElementException("Invalid field reference");    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	for (int i=0;i<td_itemAr.size();i++) {
    		if (name==null) {
    			if (td_itemAr.get(i).fieldName == null) {
    				return i;
    			} else {
    				continue;
    			}
    		} else if (td_itemAr.get(i).fieldName == null)  {
    			continue;
    		} else if (td_itemAr.get(i).fieldName.equals(name)) {
    			return i;
    		}
    	}
        throw new NoSuchElementException("No field index found for name " + name);    
    }


    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (int i=0; i<td_itemAr.size();i++) {
    		size += td_itemAr.get(i).fieldType.getLen();
    	}
    	return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	// we just have to merge the two TDItem Arrays
    	
    	ArrayList<Type> tdcombined_typelist = new ArrayList<Type>();
    	ArrayList<String> tdcombined_fieldlist = new ArrayList<String>();
    	
    	Iterator<TDItem> td1_iterator = td1.iterator();
    	while(td1_iterator.hasNext()) {
    		TDItem td_item = td1_iterator.next();
    		tdcombined_typelist.add(td_item.fieldType);
    		tdcombined_fieldlist.add(td_item.fieldName);
    	}    	
    	Iterator<TDItem> td2_iterator = td2.iterator();
    	while(td2_iterator.hasNext()) {
    		TDItem td_item  = td2_iterator.next();
    		tdcombined_typelist.add(td_item.fieldType);
    		tdcombined_fieldlist.add(td_item.fieldName);
    	}

    	String [] fieldAr = tdcombined_fieldlist.toArray(new String[tdcombined_fieldlist.size()]);
    	Type[] typeAr = tdcombined_typelist.toArray(new Type[tdcombined_typelist.size()]);
    	
    	TupleDesc td_concat = new TupleDesc(typeAr, fieldAr);
    	return td_concat;    
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
    	if (o instanceof TupleDesc) {
    		Iterator<TDItem> o_iterator = ((TupleDesc)o).iterator();
    		int td_index = 0;
    		while(o_iterator.hasNext() && td_index < td_itemAr.size()) {
    			TDItem o_item = o_iterator.next();
    			if (!o_item.fieldType.equals(td_itemAr.get(td_index).fieldType)) {
    				return false;
    			}
    			td_index++;
    		}
    		if (td_index == td_itemAr.size() && !o_iterator.hasNext()) {
    			return true;
    		}
    		return false;		
    	}
    	return false;
    }

    	
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String td_string = "";
    	for (int i=0; i<td_itemAr.size();i++) {
    		td_string += td_itemAr.get(i).toString();
    	}
    	return td_string;
    	
    }
    
    public ArrayList<TDItem> getTypeAr() {
    	return this.td_itemAr;
    }
}
