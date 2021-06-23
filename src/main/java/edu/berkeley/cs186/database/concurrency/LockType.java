package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S, // shared
    X, // exclusive
    IS, // intention shared
    IX, // intention exclusive
    SIX, // shared intention exclusive
    NL; // no lock held

    /**
     * This method checks whether lock types A and B are compatible with each other.
     * If a transaction can hold lock type A on a resource at the same time another
     * transaction holds lock type B on the same resource, the lock types are
     * compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        if (a == NL || b == NL)
            return true;

        if (a == X || b == X)
            return false;
        else if (a == IS || b == IS)
            return true;
        else if (a == SIX || b == SIX)
            return false;
        else if (a == b && (a == IX || a == S))
            return true;

        return false;
    }

    /**
     * This method returns the lock on the parent resource that should be requested
     * for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
                return IS;
            case X:
                return IX;
            case IS:
                return IS;
            case IX:
                return IX;
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a
     * childLockType on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        switch (childLockType) {
            case NL:
                return true;
            case S:
            case IS:
                if (parentLockType != NL)
                    return true;
                return false;
            case X:
            case IX:
            case SIX:
                if (parentLockType == SIX || parentLockType == X || parentLockType == IX)
                    return true;
                return false;
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation requiring
     * another lock (e.g. an S lock can be substituted with an X lock, because an X
     * lock allows the transaction to do everything the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }

        if (required == substitute)
            return true;

        // TODO(proj4_part1): implement
        switch (required) {
            case NL:
                return true;
            case S:
                if (substitute == X || substitute == SIX)
                    return true;
            case X:
                return false;
            case IS:
                if (substitute != NL)
                    return true;
                return false;
            case IX:
                if (substitute == NL || substitute == IS || substitute == S)
                    return false;
                return true;
            case SIX:
                if (substitute == X)
                    return true;
                return false;
        }
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}
