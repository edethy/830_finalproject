package simpledb;

import java.util.concurrent.*;
import java.util.*;


public class LockManager {

	// keep track of which pages are locked by which transactions
	// we need to give away locks and keep track of them and stuff.
	private ConcurrentHashMap<PageId, HashSet<TransactionId>> m_sharedLockMap;
	private ConcurrentHashMap<PageId, TransactionId> m_exclusiveLockMap;
		
	public LockManager() {
		m_sharedLockMap = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
		m_exclusiveLockMap = new ConcurrentHashMap<PageId, TransactionId>();
	}
	
	synchronized public boolean getLock(TransactionId tid, PageId pid, Permissions p) {
		boolean acquired_lock = false;
    	long start_time = System.currentTimeMillis();
		Random randomGenerator = new Random();
    	long end_time = start_time + randomGenerator.nextInt(10)*100 + 110;
    	
    	while(System.currentTimeMillis() < end_time) {
    		if (p==Permissions.READ_ONLY) {
    			acquired_lock = getSharedLock(tid, pid);
    		} else {
    			acquired_lock = getExclusiveLock(tid, pid);
    		}
    		if (acquired_lock) { break;}
    		try {
    			Thread.sleep(5);
    		} catch (InterruptedException e) {
    			e.printStackTrace();
    		}
    	}
    	return acquired_lock;

	}
	
	public boolean holdsLock(TransactionId tid, PageId pid) {
		HashSet<TransactionId> current_trans = m_sharedLockMap.get(pid);
		if (current_trans != null && current_trans.contains(tid)) {
			return true;
		}
		TransactionId tid_exclusive = m_exclusiveLockMap.get(pid);
		if (tid_exclusive != null && tid_exclusive.equals(tid)) {
			return true;
		}
		return false;
	}

	private boolean getSharedLock(TransactionId tid, PageId pid) {
		if (transactionHoldsSharedLock(tid, pid)) { return true; }
		if (m_exclusiveLockMap.containsKey(pid) && !m_exclusiveLockMap.get(pid).equals(tid)) {
			return false;
		}
		synchronized(this) {		
			HashSet<TransactionId> current_transactions;
			if (!m_sharedLockMap.containsKey(pid)) {
				current_transactions = new HashSet<TransactionId>();
			} else {
				current_transactions = m_sharedLockMap.get(pid);
			}
			current_transactions.add(tid);
			m_sharedLockMap.put(pid, current_transactions);
			return true;	
		}
	}
	
	
	private boolean getExclusiveLock(TransactionId tid, PageId pid) {
		if (transactionHoldsExclusiveLock(tid, pid)) { return true; }
		synchronized(this)
		{
			if (!m_exclusiveLockMap.containsKey(pid) && !m_sharedLockMap.containsKey(pid)) {
				m_exclusiveLockMap.put(pid, tid);
				return true;
			} else if (m_sharedLockMap.containsKey(pid)) {
				HashSet<TransactionId> current_trans = m_sharedLockMap.get(pid);
				if (current_trans.size() == 1) {
					if (current_trans.contains(tid)) {
						m_exclusiveLockMap.put(pid, tid);
						m_sharedLockMap.remove(pid);
						return true;
					}
				}
			}
			return false;			
		}
	}
	
	public void releaseLock(TransactionId tid, PageId pid) {
		if (m_sharedLockMap.containsKey(pid)) {
			HashSet<TransactionId> current_trans = m_sharedLockMap.get(pid);
			current_trans.remove(tid);
			if (current_trans.isEmpty()) {
				m_sharedLockMap.remove(pid);
			} else {
				m_sharedLockMap.put(pid, current_trans);
			}
		}
		if (m_exclusiveLockMap.containsKey(pid)) {
			if (m_exclusiveLockMap.get(pid).equals(tid)) {
				m_exclusiveLockMap.remove(pid);
			}
		}
	}
	
	public void releaseLock(TransactionId tid) {
		for (PageId p : m_sharedLockMap.keySet()) {
			if (transactionHoldsSharedLock(tid, p)) {
				releaseLock(tid, p);
			}
		}
		for (PageId p : m_exclusiveLockMap.keySet()) {
			if (transactionHoldsExclusiveLock(tid, p)) {
				releaseLock(tid, p);
			}
		}
	}
	
	private boolean transactionHoldsExclusiveLock(TransactionId tid, PageId pid) {
		if (!m_exclusiveLockMap.containsKey(pid)) {
			return false;
		} else if (m_exclusiveLockMap.get(pid).equals(tid)) {
			return true;
		}
		return false;
	}
	
	private boolean transactionHoldsSharedLock(TransactionId tid, PageId pid) {
		if (!m_sharedLockMap.containsKey(pid)) {
			return false;
		} else if (m_sharedLockMap.get(pid).contains(tid)) {
			return true;
		}
		return false;
	}	
}