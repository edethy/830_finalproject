package simpledb;

import java.util.concurrent.*;
import java.util.*;


public class LockManagerDAG {

	private class TransactionNode {
		
		public TransactionId tid;
		public ConcurrentHashMap<PageId, Permissions> page_lock_type;
		
		private TransactionNode(TransactionId tid) {
			this.tid = tid;
		}
		
		public void addPageLock(PageId pid, Permissions p) {
			page_lock_type.put(pid, p);
		}
		
		public boolean getPageLock(PageId pid) {
			for (PageId p : page_lock_type.keySet()) {
				if (p==pid) {
					return true;
				}
			}
			return false;
		}
	}
		
	private CopyOnWriteArrayList<TransactionNode> m_active_trans;
	private CopyOnWriteArrayList<TransactionNode> m_vertex_list;
	private ConcurrentHashMap<TransactionNode, TransactionNode> m_edge_list;
	
	public LockManagerDAG() {
		m_active_trans = new CopyOnWriteArrayList<TransactionNode>();
		m_vertex_list = new CopyOnWriteArrayList<TransactionNode>();
		m_edge_list = new ConcurrentHashMap<TransactionNode, TransactionNode>();
	}
	
	
	
	public boolean getLock(TransactionId tid, PageId pid, Permissions p) {

		return false;
		
	}
	
	public boolean holdsLock(TransactionId tid, PageId pid) {
		for (TransactionNode t : m_active_trans) {
			if (t.tid == tid && t.getPageLock(pid)) {
				return true;
			}
		}
		return false;
	}

	private boolean getSharedLock(TransactionId tid, PageId pid) {

		return false;
	}
	
	
	private boolean getExclusiveLock(TransactionId tid, PageId pid) {

		return false;
	}
	
	public void releaseLock(TransactionId tid, PageId pid) {

	}
	
	public void releaseLock(TransactionId tid) {
		// release all shared locks and exclusive locks this transaction holds
	}
	
	private boolean transactionHoldsExclusiveLock(TransactionId tid, PageId pid) {

		return false;
	}
	
	private boolean transactionHoldsSharedLock(TransactionId tid, PageId pid) {

		return false;
	}
	
}
