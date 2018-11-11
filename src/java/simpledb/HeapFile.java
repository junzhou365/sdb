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
    private RandomAccessFile raf;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        try {
            raf = new RandomAccessFile(f, "rw");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        int pageSize = BufferPool.getPageSize();
        long offset = pid.getPageNumber() * pageSize;
        try {
            raf.seek(offset);
            byte data[] = new byte[pageSize];
            raf.read(data);
            HeapPageId pgid = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            page = new HeapPage(pgid, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int num = -1;
        try {
            num = (int) raf.length() / BufferPool.getPageSize();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}

class HeapFileIterator extends AbstractDbFileIterator {

    private TransactionId tid;
    private HeapFile f;
    private int pgno;
    private Iterator<Tuple> pgit;

    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.tid = tid;
        this.f = f;
        pgno = 0;
    }

    // creates a tuple iterator for a given page 
    private Iterator<Tuple> pageIterator(int pgno) throws DbException, TransactionAbortedException {
        PageId pid = new HeapPageId(f.getId(), pgno);
        Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        // couldn't read more page from BufferPool
        if (page == null) {
            return null;
        }

        // explicitly downcast
        HeapPage hp = (HeapPage) page;
        return hp.iterator();
    }

    public void open() throws DbException, TransactionAbortedException {
        pgno = 0;
        pgit = pageIterator(pgno);
    }

    public Tuple readNext() throws DbException, TransactionAbortedException {
        while (pgit != null) {
            if (pgit.hasNext()) {
                return pgit.next();
            }

            pgno++;
            if (pgno >= f.numPages()) {
                return null;
            }

            pgit = pageIterator(pgno);
        }

        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void close() {
        super.close();
        pgit = null;
    }
}
