package simpledb;

import java.io.*;
import java.util.*;
import simpledb.Permissions;

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

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPage heapPage       = null;
        byte[] bytes            = new byte[BufferPool.getPageSize()];
        RandomAccessFile file   = null;
        
        try {
            file = new RandomAccessFile(this.file, "r");
            file.seek(pid.getPageNumber() * BufferPool.getPageSize());
            file.read(bytes);
            heapPage = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), bytes);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        } finally {
            try {
                file.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return heapPage;
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
        return (int) (file.length() / BufferPool.getPageSize());
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
    // Iterate all tuples in file use {@link BufferPool#getPage}
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {
        TransactionId       tid;
        int                 tableId;
        int                 pgNo;
        Iterator<Tuple>     iterator;
        HeapPage            page;
        
        public HeapFileIterator(TransactionId tid) {
            this.tid        = tid;
            this.pgNo       = 0;
            this.tableId    = getId();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.page       = (HeapPage) Database.getBufferPool().
                                getPage(tid, new HeapPageId(tableId, pgNo), Permissions.READ_ONLY);
            this.iterator   = page.iterator();           
        }

        // need to assure no empty page
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.iterator == null)
                return false;
            
            if (this.iterator.hasNext() || this.pgNo < numPages() - 1)
                return true;
            
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext())
                throw new NoSuchElementException();

            while (!this.iterator.hasNext()) {
                this.pgNo++;

                if (this.pgNo >= numPages())
                    throw new NoSuchElementException();

                this.page       = (HeapPage) Database.getBufferPool().
                                    getPage(tid, new HeapPageId(tableId, pgNo), Permissions.READ_ONLY);
                this.iterator   = this.page.iterator();
            }

            return this.iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.pgNo           = 0;
            HeapPage heapPage   = (HeapPage) Database.getBufferPool().
                                        getPage(tid, new HeapPageId(tableId, pgNo), Permissions.READ_ONLY);
            this.iterator       = heapPage.iterator();    
        }

        @Override
        public void close() {
            this.page       = null;
            this.iterator   = null;
        }
        
    }

}

