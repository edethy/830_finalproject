package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.transaction.TransactionRequiredException;

public class HeapFileIterator implements DbFileIterator {

    private static final long serialVersionUID = 1L;

	private int tableid;
	private TransactionId tid;
	private HeapFile f;
	
	private Iterator<Tuple> current_iterator;
	private Page current_page;
	private int num_pages;
	private int start_pgno = 0;
	private int start_tupno = 0;

	private boolean at_end = false;

    public HeapFileIterator(TransactionId tid, int tableid) {
    	this.tableid = tableid;
    	this.tid = tid;
    	this.f = (HeapFile)(Database.getCatalog().getDatabaseFile(tableid));    	
    	this.num_pages = this.f.numPages();
	}
	
	public HeapFileIterator(TransactionId tid, int tableid, int start_pgno) {
    	this.tableid = tableid;
    	this.tid = tid;
    	this.f = (HeapFile)(Database.getCatalog().getDatabaseFile(tableid));    	
		this.num_pages = this.f.numPages();		
		this.start_pgno=start_pgno;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		HeapPageId pid = new HeapPageId(tableid, start_pgno);
		HeapPage page = (HeapPage)(Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY));
		// System.out.println("HeapFileIterator starting on page; " + page.getId().getPageNumber());
		current_iterator = page.iterator();		
    	current_page = page;
	}	

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
    	if ((current_iterator != null && current_iterator.hasNext()) || get_next_non_empty_page() != null) {
    		return true;
    	}
    	return false;
    }

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    	try {
        	if (!current_iterator.hasNext()) {
        		HeapPage next_page = get_next_non_empty_page();
        		if (next_page != null) {
        			current_iterator = next_page.iterator();
        			current_page = next_page;
        		}
			}
        	return current_iterator.next();
    	} catch (Exception e) {
			e.printStackTrace();
    		throw new NoSuchElementException("Iterator not open");
    	}
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
    	close();
    	open();
	}

	@Override
	public void close() {
    	current_iterator = null;
	}

    private HeapPage get_next_non_empty_page() throws TransactionAbortedException, DbException {
    	if (this.current_page != null) {
        	int current_pgno = this.current_page.getId().getPageNumber();
        	for (int i=current_pgno+1;i<num_pages;i++) {
        		HeapPageId pid = new HeapPageId(this.tableid, i);
            	HeapPage page = (HeapPage)(Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY));	
            	if (page.iterator().hasNext()) {
            		return page;
            	}
        	}
    	}
    	return null;
    }
}
