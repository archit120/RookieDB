package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            for(Lock lock : locks) {
                if(lock.transactionNum == except)
                    continue;
                if(!LockType.compatible(lockType, lock.lockType))
                    return false;
            }
            // TODO(proj4_part1): implement
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lockNew) {
            Lock foundLock = null;
            for(Lock lock : locks) {
                if(lock.transactionNum == lockNew.transactionNum) {
                    foundLock = lock;
                }
            }

            if(!transactionLocks.containsKey(lockNew.transactionNum))
                transactionLocks.put(lockNew.transactionNum, new ArrayList<>());

            if(foundLock != null) 
            {
                locks.remove(foundLock);
                transactionLocks.get(foundLock.transactionNum).remove(foundLock);
            }
            locks.add(lockNew);
            transactionLocks.get(lockNew.transactionNum).add(lockNew);
            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Long transactionNumber) {
            // TODO(proj4_part1): implement
            Lock foundLock = null;
            for(Lock lock : locks) {
                if(lock.transactionNum == transactionNumber) {
                    foundLock = lock;
                }
            }

            if(foundLock != null) 
            {
                locks.remove(foundLock);
                transactionLocks.get(foundLock.transactionNum).remove(foundLock);
            }
            else
                throw new NoLockHeldException("No locks held by transaction: " +transactionNumber);
            return;
        }

        public void releaseLock(Lock lock) {
            getResourceEntry(lock.name).releaseLock(lock.transactionNum);
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if(addFront)
                waitingQueue.addFirst(request);
            else
                waitingQueue.addLast(request);
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while(requests.hasNext()) {
                LockRequest lockRequest = requests.next();
                if(checkCompatible(lockRequest.lock.lockType, lockRequest.lock.transactionNum)) {
                    waitingQueue.removeFirst();
                    for(Lock lock : lockRequest.releasedLocks)
                        releaseLock(lock);
                    grantOrUpdateLock(lockRequest.lock);

                    lockRequest.transaction.unblock();
                }
                else
                    break;
            }
            // TODO(proj4_part1): implement
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            LockType ret = LockType.NL;
            for(Lock lock : locks) {
                if(lock.transactionNum == transaction)
                    ret = lock.lockType;
            }
            return ret;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            List<Lock> releasedLocks = new ArrayList<>();
            for(ResourceName resourceName : releaseNames) {
                releasedLocks.add(new Lock(resourceName, LockType.NL, transaction.getTransNum()));
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());


            LockRequest lockRequest = new LockRequest(transaction, lock, releasedLocks);
            ResourceEntry resource = getResourceEntry(name);

            LockType prevLockType = getLockType(transaction, name);
            if(lockType==prevLockType)
                throw new DuplicateLockRequestException("");


            if(resource.checkCompatible(lockType, transaction.getTransNum())  && resource.waitingQueue.size() == 0) {
                resource.addToQueue(lockRequest, true);
                resource.processQueue();

            }
            else
            {
                resource.addToQueue(lockRequest, false); shouldBlock = true;
                transaction.prepareBlock();
                

            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    
    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest lockRequest = new LockRequest(transaction, lock);
            ResourceEntry resource = getResourceEntry(name);

            LockType prevLockType = getLockType(transaction, name);
            if(lockType==prevLockType)
                throw new DuplicateLockRequestException("");
            
            if(resource.checkCompatible(lockType, transaction.getTransNum()) && resource.waitingQueue.size() == 0) {
                resource.addToQueue(lockRequest, true);
                resource.processQueue();
            }
            else
            {
                resource.addToQueue(lockRequest, false); shouldBlock = true;
                transaction.prepareBlock();
            }

        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            resourceEntry.releaseLock(transaction.getTransNum());
            resourceEntry.processQueue();
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            LockType prevLockType = getLockType(transaction, name);
            if(prevLockType == LockType.NL)
                throw new NoLockHeldException("");
            if(!LockType.substitutable(newLockType, prevLockType))
                throw new InvalidLockException("");
            if(newLockType==prevLockType)
                throw new DuplicateLockRequestException("");
            
            ResourceEntry resourceEntry = getResourceEntry(name);
            resourceEntry.addToQueue(new LockRequest(transaction, new Lock(name, newLockType, transaction.getTransNum())), true);
            if(resourceEntry.checkCompatible(newLockType, transaction.getTransNum())) 
                resourceEntry.processQueue();
            else
            {
                shouldBlock=true;
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
