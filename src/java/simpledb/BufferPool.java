package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private int numPages;
    private ConcurrentHashMap<PageId, Page> pageMap;

    private ConcurrentHashMap<TransactionId, ArrayList<PageId>> tPages;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private LockManager lockmngr;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageMap = new ConcurrentHashMap<>();
        tPages = new ConcurrentHashMap<>();
        lockmngr = new LockManager();
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

    private Page loadPage(PageId pid) {
        int tableid = pid.getTableId();
        Catalog catalog = Database.getCatalog();
        DbFile df = catalog.getDatabaseFile(tableid);
        return df.readPage(pid);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        lockmngr.acquire(pid, tid, perm);

        tPages.putIfAbsent(tid, new ArrayList<>());
        // TODO: faster
        if (!tPages.get(tid).contains(pid) && perm == Permissions.READ_WRITE) {
            tPages.get(tid).add(pid);
        }
        
        if (pageMap.containsKey(pid)) {
            return pageMap.get(pid);
        }

        if (pageMap.size() == numPages) {
            evictPage();
        }

        Page page = loadPage(pid);
        if (page == null) {
            return null;
        }

        pageMap.put(pid, page);

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockmngr.release(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockmngr.hasLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // tests tear down with this call
        if (!tPages.containsKey(tid)) {
            return;
        }

        if (commit) {
            flushPages(tid);
        } else {
            // revert
            for (PageId pid: tPages.get(tid)) {
                pageMap.put(pid, loadPage(pid));
            }
        }

        for (PageId pid: tPages.get(tid)) {
            lockmngr.release(pid, tid);
        }
        tPages.remove(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages =
            Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);

        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            page.markDirty(true, tid);
            if (!pageMap.containsKey(page.getId())) {
                if (pageMap.size() == numPages) {
                    evictPage();
                }
            }
            // Overwrite any existing pages
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(pid.getTableId()).deleteTuple(tid, t);

        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            page.markDirty(true, tid);
            if (!pageMap.containsKey(page.getId())) {
                if (pageMap.size() == numPages) {
                    evictPage();
                }
            }
            // Overwrite any existing pages
            pageMap.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (ConcurrentHashMap.Entry<PageId, Page> entry: pageMap.entrySet()) {
            flushPage(entry.getKey());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (!pageMap.containsKey(pid))
            return;

        Page page = pageMap.get(pid);
        // if (page.isDirty() == null)
        //     return;

        Catalog catalog = Database.getCatalog();
        DbFile df = catalog.getDatabaseFile(pid.getTableId());
        df.writePage(page);

        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid: tPages.get(tid)) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        for (ConcurrentHashMap.Entry<PageId, Page> entry: pageMap.entrySet()) {
            if (entry.getValue().isDirty() != null) {
                continue;
            }

            PageId pid = entry.getKey();
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }

            discardPage(pid);
            return;
        }
        
        throw new DbException("no clean pages to evict");
    }

}

/**
 * LockManager protects the whole database
 */
class LockManager {
    /**
     * Latch protects page, thread-safe
     */
    class Latch {
        PageId pid;
        TransactionId writer;
        HashSet<TransactionId> readers;

        boolean debug;

        Latch(PageId pid) {
            this.pid = pid;
            readers = new HashSet<>();
            debug = false;
        }

        private void log(TransactionId tid, int type, boolean toAcquire) {
            if (!debug) {
                return;
            }

            String lockType = "";
            switch (type) {
            case 0:
                lockType = "exclusive lock";
                break;
            case 1:
                lockType = "shared lock";
                break;
            case 2:
                lockType = "upgrade lock";
                break;
            case 3:
                lockType = "reentrant lock";
                break;
            case 4:
                lockType = "already got excl lock";
                break;
            }

            if (toAcquire) {
                System.out.printf("acquired %s on pgno:%s for tid:%d\n", lockType, pid.getPageNumber(), tid.getId());
            } else {
                System.out.printf("released %s on pgno:%s for tid:%d\n", lockType, pid.getPageNumber(), tid.getId());
            }
        }

        private synchronized boolean acquire(TransactionId tid, boolean exclusive) {
            if (exclusive && readers.size() == 0 && writer == null) {
                writer = tid;
                log(tid, 0, true);
                return true;
            }

            if (exclusive && writer == null && readers.size() == 1 && readers.contains(tid)) {
                writer = tid;
                readers.clear();
                log(tid, 2, true);
                return true;
            }

            if (!exclusive && writer == null) {
                readers.add(tid);
                log(tid, 1, true);
                return true;
            }

            if (writer != null && writer.equals(tid)) {
                log(tid, 4, true);
                return true;
            }

            // reentrant reader
            if (readers.contains(tid) && !exclusive) {
                log(tid, 3, true);
                return true;
            }

            return false;
        }

        synchronized boolean release(TransactionId tid) {
            if (writer != null && writer.equals((tid))) {
                writer = null;
                log(tid, 0, false);
                return true;
            }

            if (readers.contains(tid)) {
                readers.remove(tid);
                log(tid, 1, false);
                return true;
            }

            return false;
        }

        synchronized boolean hasLock(TransactionId tid) {
            return writer != null && writer.equals(tid) || readers.contains(tid);
        }
    }

    private HashMap<PageId, Latch> latches;
    private HashMap<PageId, HashSet<TransactionId>> contenders;
    private HashMap<TransactionId, HashSet<PageId>> holders;

    LockManager() {
        latches = new HashMap<>();
        contenders = new HashMap<>();
        holders = new HashMap<>();
    }

    void acquire(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {
        for (int i = 0; !tryAcquire(pid, tid, perm);) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // just tryAcquire again
            }
            
            // timeout 100ms
            if (i >= 10) {
                if (tryAbort(pid, tid)) {
                    // System.out.printf("pid:%s, tid:%d, perm:%s contending too long, abort txn\n",
                    //     pid.getPageNumber(), tid.getId(), perm);
                    throw new TransactionAbortedException();
                }
                i = 0;
            }

            i++;
        }
    }

    synchronized void addContender(PageId pid, TransactionId tid) {
        contenders.putIfAbsent(pid, new HashSet<>());
        contenders.get(pid).add(tid);
    }

    synchronized void addHolder(PageId pid, TransactionId tid) {
        holders.putIfAbsent(tid, new HashSet<>());
        holders.get(tid).add(pid);
    }

    synchronized boolean tryAbort(PageId pid, TransactionId tid) {
        HashSet<TransactionId> txns = contenders.get(pid);
        TransactionId winner = null;
        for (TransactionId atid: txns) {
            // randomly select txn to win
            if (winner == null) {
                winner = atid;
                continue;
            }
            for (PageId tpid: holders.get(atid)) {
                release(tpid, atid);
            }
        }

        boolean aborted =  winner != tid;

        txns.clear();
        txns.add(winner);
        return aborted;
    }

    synchronized boolean tryAcquire(PageId pid, TransactionId tid, Permissions perm) {
        addContender(pid, tid);

        latches.putIfAbsent(pid, new Latch(pid));
        boolean acquired = latches.get(pid).acquire(tid, perm == Permissions.READ_WRITE);
        if (acquired) {
            addHolder(pid, tid);
            contenders.get(pid).remove(tid);
        }
        return acquired;
     }

    synchronized boolean release(PageId pid, TransactionId tid) {
        latches.putIfAbsent(pid, new Latch(pid));
        boolean released = latches.get(pid).release(tid);
        if (released) {
            // probably too slow
            holders.get(tid).remove(pid);
        }
        return released;
    }

    synchronized boolean hasLock(PageId pid, TransactionId tid) {
        latches.putIfAbsent(pid, new Latch(pid));
        return latches.get(pid).hasLock(tid);
    }
}