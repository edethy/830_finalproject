package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File f;
	private TupleDesc td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
    	return this.f;

    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	if (pid.getPageNumber()==numPages()) {
    		try {
    			HeapPage p = new HeapPage((HeapPageId)pid, new byte[BufferPool.getPageSize()]);
    			writePage(p);
    			return p;
    		} catch(IOException e) {
                System.out.println("IOException caught writing page: " + pid.getPageNumber());                
    			System.out.println("IO Exception caught " + e);
    			return null;
    		}
        }
        long file_offset = 0;
    	try {
    		file_offset = pid.getPageNumber() * BufferPool.getPageSize();
    		byte byte_array[] = new byte[BufferPool.getPageSize()];
            RandomAccessFile file_access = new RandomAccessFile(f, "r");
            if (file_offset < 0)
                file_offset = -1 * file_offset;
    		file_access.seek(file_offset);
    		file_access.readFully(byte_array);
    		file_access.close();
    		HeapPage page = new HeapPage((HeapPageId)pid, byte_array);
    		return page;
    	} catch(IOException e) {
            System.out.println("IOException caught reading page: " + pid.getPageNumber() + " Using offset: " + file_offset + " Num Pages: " + numPages());
    		e.printStackTrace(System.out);
    		return null;
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	RandomAccessFile raf = new RandomAccessFile(f, "rw");
        int offset = ((HeapPage)page).getId().getPageNumber() * BufferPool.getPageSize();
        System.out.println("Trying to access offset: " + offset);
    	raf.seek(offset);
    	raf.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(this.f.length()/BufferPool.getPageSize());

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> p_list = new ArrayList<Page>();
    	for (int i = 0; i<numPages()+1;i++) {
    		HeapPageId pid = new HeapPageId(getId(), i);
    		HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    		if (p.getNumEmptySlots() > 0) {
    			// Upgrade to READ_WRITE
    			p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    			p.insertTuple(t);
    			p_list.add(p);
    			return p_list;
    		} else {
    			// We release the page if we aren't going to add to it
    			Database.getBufferPool().releasePage(tid, pid);
    		}
    	}
    	throw new DbException("Unable to add tuple");
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> p_list = new ArrayList<Page>();
    	HeapPageId pid = (HeapPageId)t.getRecordId().getPageId();
    	HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	p.deleteTuple(t);
    	p_list.add(p);
    	return p_list;
   }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	HeapFileIterator it = new HeapFileIterator(tid, getId());
    	return it;
    }

    public DbFileIterator iterator(TransactionId tid, int start_pgno) {
    	HeapFileIterator it = new HeapFileIterator(tid, getId(), start_pgno);
    	return it;
    }
}

