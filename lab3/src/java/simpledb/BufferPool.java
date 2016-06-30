package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Enumeration;
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
    private static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private static int pageSize = PAGE_SIZE;

    private class PageBufferPool {
        private final int capacity;
        private final ConcurrentHashMap<PageId, Page> pageIdxMap;
        private final ArrayList<Page> pageBuffer;

        public PageBufferPool(int cap) {
            this.capacity = cap;
            this.pageIdxMap = new ConcurrentHashMap<>(cap);
            this.pageBuffer = new ArrayList<>(cap);
        }

        public boolean containsKey(PageId k) {
            return pageIdxMap.containsKey(k);
        }

        public Page get(PageId k) {
                // move to last
                // Page pg = pageBuffer.remove(idx);
                // pageBuffer.add(pg);
                // pageIdxMap.put(k, pageBuffer.size()-1);
            return pageIdxMap.get(k);
        }

        public Page put(PageId pid, Page pg) {
            if (pageBuffer.size() < capacity) {
                if (pageIdxMap.containsKey(pid)) {
                    pageBuffer.remove(pg);
                    pageBuffer.add(pg);
                } else {
                    pageIdxMap.put(pid, pg);
                    pageBuffer.add(pg);
                }
            }
            return null;
        }

        public Page remove(PageId pid) {
            if (pageIdxMap.containsKey(pid)) {
                // int idx = pageIdxMap.get(pid);
                // Page pg = pageBuffer.get(idx);
                // System.out.println(idx);
                // System.out.println("size: " + size() + " cap: " + getCapacity());
                    // assert pageBuffer.remove(pg);
                    // pageBuffer.remove(0);
                Page pg = pageIdxMap.remove(pid);
                pageBuffer.remove(pg);
                return pg;
            }
            return null;
        }

        public int size() {
            assert pageBuffer.size() == pageIdxMap.size();
            return pageBuffer.size();
        }

        /*
        * @return page or null if not exist
        * */
        public Page evictPage() throws DbException {
            // LRU policy
            int bufferSize = pageBuffer.size();
            if (bufferSize > 0 && bufferSize == capacity) {
                Page pg = pageBuffer.get(0);
                // pageIdxMap.remove(pg.getId());
                return pg;
            }
            return null;
        }

        public int getCapacity() {
            return capacity;
        }

    }
    private final int numPages;
    private final PageBufferPool bufferPool;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.bufferPool = new PageBufferPool(numPages);
    }
    
    public static int getPageSize() {
      return PAGE_SIZE;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (bufferPool.containsKey(pid)) {
            return bufferPool.get(pid);
        } else {
            Page page = Database.getCatalog()
                        .getDatabaseFile(pid.getTableId())
                        .readPage(pid);

            if (bufferPool.size() == numPages) {
                evictPage();
            }

            assert bufferPool.size() < numPages;
            bufferPool.put(page.getId(), page);
            return page;
        }
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
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
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
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affectedPgs = dbFile.insertTuple(tid, t);
        for (Page page : affectedPgs) {
            page.markDirty(true, tid);
            // bufferPool.replace(page.getId(), page);
            // bufferPool.remove(page.getId());
            bufferPool.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> affectedPgs = dbFile.deleteTuple(tid, t);
        for (Page pg : affectedPgs) {
            pg.markDirty(true, tid);
            // bufferPool.remove(affectedPg.getId());
            bufferPool.put(pg.getId(), pg);
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
        Enumeration<PageId> it = bufferPool.pageIdxMap.keys();
        while (it.hasMoreElements()) {
            flushPage(it.nextElement());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (bufferPool.pageIdxMap.containsKey(pid)) {
            Page pg = bufferPool.pageIdxMap.get(pid);
            if (pg.isDirty() != null) {
                Database.getCatalog().getDatabaseFile(pg.getId().getTableId()).writePage(pg);
            }
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page pg = bufferPool.evictPage();
        PageId pid = pg.getId();
        try {
            if (pg != null) {
                flushPage(pid);
                discardPage(pid);
            }
        } catch (IOException e) {
            throw new DbException("evictPage: unable to error when flush a page");
        }
    }

}
