package simpledb;
import java.io.*;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

	private OpIterator insert_tuples;
	private TransactionId tid;
	private int tableid;
	private TupleDesc td;
	private int insert_tuple_count;
	private boolean insert_complete;
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
    	this.insert_tuples = child;
    	this.tid = t;
    	this.tableid = tableId;
    	TupleDesc child_td = child.getTupleDesc();
    	this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    	if (!child_td.equals(Database.getCatalog().getTupleDesc(tableid))) {
    		throw new DbException("TupleDesc of child differs from table");
    	}
    	this.insert_tuple_count = 0;
    	this.insert_complete = false;
    }

    public TupleDesc getTupleDesc() {
    	return td;

    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	insert_tuples.open();
    }

    public void close() {
        insert_tuples.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        insert_tuples.close();
        insert_tuples.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (insert_complete) { return null; }
    	Tuple t = new Tuple(td);
    	try {
        	while (insert_tuples.hasNext()) {
        		Tuple next_tuple = insert_tuples.next();
        		Database.getBufferPool().insertTuple(tid, tableid, next_tuple);
        		insert_tuple_count++;
        	}
    	} catch (IOException e) {System.out.println("Exception caught reading tuples " + e); }
		t.setField(0, new IntField(insert_tuple_count)); 
		insert_complete = true;
		return t;
    }

    @Override
    public OpIterator[] getChildren() {
    	return new OpIterator[] {this.insert_tuples};
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	this.insert_tuples = children[0];
    }
}
