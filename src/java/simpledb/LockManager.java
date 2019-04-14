package simpledb;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * LockManager protects the whole database
 */
public class LockManager {
    class Logger {

        boolean debug;
        java.util.logging.Logger jlogger;
        SimpleDateFormat df;

        Logger(boolean debug) {
            this.debug = debug;

            jlogger = java.util.logging.Logger.getLogger("LockLogger");  
            FileHandler fh;
            df = new SimpleDateFormat("SSSS");

            try {  
                fh = new FileHandler("test.out");  
                jlogger.addHandler(fh);
                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);  
            } catch (Exception e) {
                e.printStackTrace();
            }
            
        }

        public void log(String format, Object ... args) {
            if (!debug) {
                return;
            }

            String ds = df.format(new java.util.Date());

            String raw = String.format(format, args);
            String msg = String.format("%s --- %s", ds, raw);
            jlogger.log(Level.INFO, msg);
        }
    }
    /**
     * Latch protects page, thread-safe
     */
    class Latch {
        PageId pid;
        TransactionId writer;
        HashSet<TransactionId> readers;

        Logger logger;

        

        Latch(Logger logger, PageId pid) {
            this.pid = pid;
            readers = new HashSet<>();
            this.logger = logger;
        }

        private void log(TransactionId tid, int type, boolean toAcquire) {
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
                logger.log("acquired %s on pgno:%s for tid:%d\n", lockType, pid.getPageNumber(), tid.getId());
            } else {
                logger.log("released %s on pgno:%s for tid:%d\n", lockType, pid.getPageNumber(), tid.getId());
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

            logger.log("txn:%s failed to acquire lock on pid:%d exclusive: %s, holder:%s\n",
                tid, pid.getPageNumber(), exclusive, holders());

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

        synchronized Permissions hasLockType(TransactionId tid) {
            if (writer != null && writer.equals(tid)) {
                return Permissions.READ_WRITE;
            }

            if (readers.contains(tid)) {
                return Permissions.READ_ONLY;
            }
            return null;
        }

        synchronized ArrayList<TransactionId> holders() {
            ArrayList<TransactionId> out = new ArrayList<>();
            if (writer != null) {
                out.add(writer);
            }

            for (TransactionId tid: readers) {
                out.add(tid);
            }

            return out;
        }
    }

    class User {
        HashSet<PageId> holdings;
        HashSet<PageId> contendings;
        TransactionId id;

        User(TransactionId id) {
            this.id = id;
            holdings = new HashSet<>();
            contendings = new HashSet<>();
        }

        public void contend(PageId pid) {
            contendings.add(pid);
        }

        public void hold(PageId pid) {
            holdings.add(pid);
        }

        public void removeHolding(PageId pid) {
            holdings.remove(pid);
        }

        public boolean hasHolding(PageId pid) {
            return holdings.contains(pid);
        }

        public HashSet<PageId> getHoldings() {
            return holdings;
        }

        public HashSet<PageId> getContendings() {
            return contendings;
        }
    }

    private HashMap<PageId, Latch> latches;
    private HashMap<PageId, HashSet<TransactionId>> pageContenders;
    private HashMap<TransactionId, User> users;

    private Logger logger;

    public LockManager() {
        latches = new HashMap<>();
        pageContenders = new HashMap<>();
        users = new HashMap<>();
        logger = new Logger(false);
    }

    void acquire(PageId pid, TransactionId tid, Permissions perm) throws TransactionAbortedException {
        addContender(pid, tid);

        for (int i = 0; !tryAcquire(pid, tid, perm);) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // just tryAcquire again
            }

            // timeout 100ms
            if (i >= 10) {
                if (tryAbort(pid, tid)) {
                    logger.log("pid:%s, tid:%d, perm:%s contending too long, abort txn\n",
                        pid.getPageNumber(), tid.getId(), perm);
                    // throw new Error("sss");
                    throw new TransactionAbortedException();
                }
                i = 0;
            }

            i++;
        }
    }

    synchronized void addContender(PageId pid, TransactionId tid) {
        pageContenders.putIfAbsent(pid, new HashSet<>());
        pageContenders.get(pid).add(tid);
        users.putIfAbsent(tid, new User(tid));
        users.get(tid).contend(pid);
    }

    synchronized void addHolder(PageId pid, TransactionId tid) {
        users.putIfAbsent(tid, new User(tid));
        users.get(tid).hold(pid);
    }

    synchronized boolean tryAbort(PageId pid, TransactionId tid) {

        logger.log("txn: %s, tid:%d try abort pid:%d\n",
            tid, tid.getId(), pid.getPageNumber());
        
        // abort others
        HashSet<TransactionId> atids = new HashSet<>();
        atids.addAll(latches.get(pid).holders());
        if (pageContenders.containsKey(pid)) {
            atids.addAll(pageContenders.get(pid));
        }

        if (!atids.contains(tid)) {
            return true;
        }

        for (TransactionId atid: atids) {
            if (atid.equals(tid)) {
                continue;
            }
            dropUserByTid(atid);
        }

        logger.log("after destroy, txn %s, tid:%d || holders:%s, pc: %s\n",
            tid, tid.getId(), latches.get(pid).holders(), pageContenders);
        
        return false;
    }

    synchronized boolean tryAcquire(PageId pid, TransactionId tid, Permissions perm) {
        if (!pageContenders.get(pid).contains(tid)) {
            return false;
        }
        
        latches.putIfAbsent(pid, new Latch(logger, pid));
        boolean acquired = latches.get(pid).acquire(tid, perm == Permissions.READ_WRITE);
        if (acquired) {
            addHolder(pid, tid);
            // pageContenders.put(pid, new HashSet<>());
        }

        logger.log("txn:%s, %d try acquire pid:%d, acquired: %s, contenders: %s\n",
            tid, tid.getId(), pid.getPageNumber(), acquired, pageContenders);
        return acquired;
     }

    synchronized boolean release(PageId pid, TransactionId tid) {
        latches.putIfAbsent(pid, new Latch(logger, pid));
        boolean released = latches.get(pid).release(tid);
        if (released) {
            // probably too slow
            users.get(tid).removeHolding(pid);
        }
        return released;
    }

    synchronized boolean hasLock(PageId pid, TransactionId tid) {
        latches.putIfAbsent(pid, new Latch(logger, pid));
        return latches.get(pid).hasLockType(tid) != null;
    }

    synchronized ArrayList<PageId> affectedPages(TransactionId tid) {
        ArrayList<PageId> out = new ArrayList<>();
        if (!users.containsKey(tid)) {
            return out;
        }

        for (PageId pid: users.get(tid).getHoldings()) {
            if (latches.get(pid).hasLockType(tid) == Permissions.READ_WRITE) {
                out.add(pid);
            }
        }

        return out;
    }

    // release all holding locks and contendings
    synchronized boolean dropUserByTid(TransactionId tid) {
        if (!users.containsKey(tid)) {
            return false;
        }

        boolean o = false;

        ArrayList<PageId> pages = new ArrayList<>();
        for (PageId pid: users.get(tid).getHoldings()) {
            pages.add(pid);
        }

        for (PageId pid: pages) {
            o |= release(pid, tid);
        }

        for (PageId pid: users.get(tid).getContendings()) {
            pageContenders.get(pid).remove(tid);
        }

        users.remove(tid);
        return o;
    }
}