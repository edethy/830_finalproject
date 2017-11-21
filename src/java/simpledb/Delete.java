package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

	private OpIterator delete_tuples;
	private TransactionId tid;
	private TupleDesc td;
	private int delete_tuple_count;
	private boolean delete_done;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
    	this.delete_tuples = child;
    	this.tid = t;
    	TupleDesc child_td = child.getTupleDesc();
    	this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    	this.delete_tuple_count = 0;
    }

    public TupleDesc getTupleDesc() {
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	delete_tuples.open();
    }

    public void close() {
    	delete_tuples.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	delete_tuples.close();
    	delete_tuples.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (delete_done) { return null; }
    	Tuple t = new Tuple(td);
    	try {
        	while (delete_tuples.hasNext()) {
        		Tuple next_tuple = delete_tuples.next();
        		Database.getBufferPool().deleteTuple(tid, next_tuple);
        		delete_tuple_count++;
        	}
    		t.setField(0, new IntField(delete_tuple_count)); 
    		delete_done = true;
    		return t;
    	} catch (IOException e) {
    		System.out.println("Exception caught reading tuples " + e);
    		return null;
    	}
    }

    @Override
    public OpIterator[] getChildren() {
    	return new OpIterator[] {this.delete_tuples};
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	this.delete_tuples = children[0];
    }

}
