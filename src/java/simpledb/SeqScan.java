package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

	private int tableid;
	private String tableAlias;
	private TransactionId tid;
	private HeapFile f;
    private int start_pgnum;
    
	private DbFileIterator current_iterator;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this.tableid = tableid;
    	this.tid = tid;
    	this.tableAlias = tableAlias;
    	this.f = (HeapFile)(Database.getCatalog().getDatabaseFile(tableid));    	
    }

    public SeqScan(TransactionId tid, int tableid, String tableAlias, int start_page_num) {
        this.tableid = tableid;
    	this.tid = tid;
    	this.tableAlias = tableAlias;
        this.f = (HeapFile)(Database.getCatalog().getDatabaseFile(tableid));    
        this.start_pgnum = start_page_num;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        String table_name = Database.getCatalog().getTableName(tableid);
    	return table_name;
        
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
    	return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
    	this.tableid = tableid;
    	this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
    	current_iterator = f.iterator(tid);
    	current_iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	ArrayList<String> td_fieldAr = new ArrayList<String>();
    	ArrayList<Type> td_typeAr = new ArrayList<Type>();

    	TupleDesc td_from_file = this.f.getTupleDesc();
    	Iterator<TupleDesc.TDItem> td_iter = td_from_file.iterator();
    	while(td_iter.hasNext()) {
    		TupleDesc.TDItem td_item = td_iter.next();
    		td_typeAr.add(td_item.fieldType);
    		String field_name = tableAlias + "." + td_item.fieldName;
    		td_fieldAr.add(field_name);
    	}
    	String [] fieldAr = td_fieldAr.toArray(new String[td_fieldAr.size()]);
    	Type[] typeAr = td_typeAr.toArray(new Type[td_typeAr.size()]);
    	TupleDesc new_td = new TupleDesc(typeAr, fieldAr);
    	return new_td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
    	return current_iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	return current_iterator.next();
    }

    public void close() {
    	current_iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	close();
    	open();
    }
}
