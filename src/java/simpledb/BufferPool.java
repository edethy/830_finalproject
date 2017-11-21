
package simpledb;

import java.io.*;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

	private static int pageSize = DEFAULT_PAGE_SIZE;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	private int num_pages;
	private ConcurrentHashMap<PageId, Page> pages;
	
    private LockManager lm;
    
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> trans_locks;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		this.num_pages = numPages;
		this.pages = new ConcurrentHashMap<PageId, Page>();
    	lm = new LockManager();
    	trans_locks = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = DEFAULT_PAGE_SIZE;
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, a page should be evicted and the new page should be added in
	 * its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
    	long start_time = System.currentTimeMillis();
		Random randomGenerator = new Random();
    	
//    	while(!lm.getLock(tid, pid, perm)) {
//    		int randomInt = randomGenerator.nextInt(20) + 5;
//    		if (System.currentTimeMillis() > end_time) {
//        		System.out.println("Aborting Transaction: " + tid + " Attempting to acquire page: " + pid+ " With perm" +perm);
//    			throw new TransactionAbortedException();
//    		}
//    		try {
//    			Thread.sleep(randomInt);
//    		} catch (Exception e) {
//    		}
//    	}
    	boolean acquired_lock = lm.getLock(tid,  pid,  perm);
    //	boolean acquired_lock = true;    	
		if(!acquired_lock) {
			throw new TransactionAbortedException();
		}
    	if (pages.containsKey(pid)) {
    		return pages.get(pid);
    	} else if(pages.size() == this.num_pages) {
    		if (!evictPage()) {
    			try {
    				transactionComplete(tid, false);
    			} catch(IOException e) {
    				e.printStackTrace();
    				throw new DbException("Unable to read page");
    			}
    			throw new DbException("Unable to evict page to get a new page");
    		}
    	}
		int table_id = pid.getTableId();
		DbFile file = Database.getCatalog().getDatabaseFile(table_id);
		Page p = file.readPage(pid);
		if (p == null) {
			throw new DbException("Unable to get page");
		}
		pages.put(pid, p);
		return p;
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
    	lm.releaseLock(tid, pid);

	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
    	lm.releaseLock(tid);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
        return lm.holdsLock(tid, p);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
    	if (commit) {
    		flushPages(tid);
    	} else {
    		evictDirtyPage(tid);
    	}
    	lm.releaseLock(tid);
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2). May
	 * block if the lock(s) cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> dirty_pages = f.insertTuple(tid, t);
		for (int i = 0; i < dirty_pages.size(); i++) {
			HeapPage p = (HeapPage) dirty_pages.get(i);
			p.markDirty(true, tid);
			pages.put(p.getId(), p);
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from and any other pages that are
	 * updated. May block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have been
	 * dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction deleting the tuple.
	 * @param t
	 *            the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
		int tableid = t.getRecordId().getPageId().getTableId();
		HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
		ArrayList<Page> dirty_pages = f.deleteTuple(tid, t);
		for (int i = 0; i < dirty_pages.size(); i++) {
			HeapPage p = (HeapPage) dirty_pages.get(i);
			p.markDirty(true, tid);
			pages.put(p.getId(), p);
		}
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {
    	for (Page p : pages.values()) {
    		if (p.isDirty() != null) {
    			flushPage(p.getId());
    		}
    	}

	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from
	 * the cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		pages.remove(pid);
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private void flushPage(PageId pid) throws IOException {
    	int tableid = pid.getTableId();
    	HeapFile f = (HeapFile)Database.getCatalog().getDatabaseFile(tableid);
    	f.writePage(pages.get(pid));
    	pages.get(pid).markDirty(false,  null);
//    	pages.remove(pid);
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
    	for (Page p : pages.values()) {
    		if (p.isDirty() != null && p.isDirty().equals(tid)) {
    			flushPage(p.getId());
    		}
    	}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized boolean evictPage() throws DbException {
    	for (Page p : pages.values()) {
    		if (p.isDirty() == null) {
    			pages.remove(p.getId());
    			return true;
    		}
    	}
    	return false;
	}
    private synchronized void evictDirtyPage(TransactionId tid) {
    	for (Page p : pages.values()) {
    		if (p.isDirty() != null && p.isDirty().equals(tid)) {
    			pages.remove(p.getId());
    		}
    	}
    }
	

}