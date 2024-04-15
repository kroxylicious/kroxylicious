/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;

/**
 * <p>This counter exists to track different usages of a {@link Dek} so that we can
 * destroy the underlying Key Material as soon as there are no cryptors using it.
 * We identify two distinct usage types of a {@link Dek}, encryption and decryption.
 * This classes responsibilities are:
 * <ul>
 *     <li>acquiring ( or denying ) new usages for each usage type</li>
 *     <li>tracking outstanding usages for each usage type as usages are acquired and later released</li>
 *     <li>tracking whether the application has destroyed each usage type</li>
 *     <li>reporting when the end state has been reached (both usage types destroyed and with 0 usages outstanding)</li>
 * </ul>
 * </p>
 * <p>Usages of a type can be acquired as long as that type is not destroyed.</p>
 * <p>We use an AtomicLong, but it's really an atomic pair of ints acting as
 * reference counts for the number of outstanding encryptors and decryptors.
 * The counts start from 1 (i.e. 1 means no outstanding cryptor), increments with
 * each cryptor created and decrements with each cryptor closed.
 * Calling destroyFor(En|De)crypt flips the sign of the count
 * and inverts the direction of the counting (i.e. cryptor close now increments).
 * This means that we hit -1 when all outstanding cryptors have been closed.</p>
 *
 * <p>Here's a worked example:</p>
 * <table>
 *     <tr><th>Action</th>                          <th>Encryptor Count</th> <th>Decryptor Count</th></tr>
 *     <tr><td>«{@link #START}»</td>                <td>1</td>               <td>1</td>    </tr>
 *     <tr><td>{@link #acquireEncryptorUsage()}</td> <td>2</td>               <td>1</td>    </tr>
 *     <tr><td>{@link #acquireEncryptorUsage()}</td> <td>3</td>               <td>1</td>    </tr>
 *     <tr><td>{@link #acquireDecryptorUsage()}</td> <td>3</td>               <td>2</td>    </tr>
 *     <tr><td>{@link #releaseEncryptorUsage()}</td><td>2</td>               <td>2</td>    </tr>
 *     <tr><td>{@link #destroyForEncrypt()}</td>    <td>-2</td>              <td>2</td>    </tr>
 *     <tr><td>{@link #releaseEncryptorUsage()}</td><td>-1</td>              <td>2</td>    </tr>
 *     <tr><td>{@link #destroyForDecrypt()}</td>    <td>-1</td>              <td>-2</td>   </tr>
 *     <tr><td>{@link #releaseDecryptorUsage()}</td><td>-1</td>              <td>-1</td>    </tr>
 *     <tr><td colspan="3">«{@link #END}» // key gets destroyed </td></tr>
 * </table>
 */
class AtomicCryptorUsageCounter {
    private static final long START = combine(1, 1);
    private static final long END = combine(-1, -1);
    private final AtomicLong state = new AtomicLong(START);

    /**
     * @return true if encryptor usage acquired
     */
    public boolean acquireEncryptorUsage() {
        long updated = state.updateAndGet(AtomicCryptorUsageCounter::acquireEncryptorUsage);
        return encryptorCount(updated) >= 0;
    }

    /**
     * @return true if decryptor usage acquired
     */
    public boolean acquireDecryptorUsage() {
        long updated = state.updateAndGet(AtomicCryptorUsageCounter::acquireDecryptorUsage);
        return decryptorCount(updated) >= 0;
    }

    /**
     * @return true if this counter is in the ended state after applying the release
     * @throws IllegalStateException if we have zero outstanding Encryptor usages
     */
    public boolean releaseEncryptorUsage() {
        return isEnded(AtomicCryptorUsageCounter::releaseEncryptorUsage);
    }

    /**
     * @return true if this counter is in the ended state after applying the release
     * @throws IllegalStateException if we have zero outstanding Decryptor usages
     */
    public boolean releaseDecryptorUsage() {
        return isEnded(AtomicCryptorUsageCounter::releaseDecryptorUsage);
    }

    /**
     * After operation is applied, future {@link #acquireEncryptorUsage()} calls will return false.
     * @return true if this counter is in the ended state after destroying
     */
    public boolean destroyForEncrypt() {
        return isEnded(AtomicCryptorUsageCounter::commenceDestroyForEncrypt);
    }

    /**
     * After operation is applied, future {@link #acquireDecryptorUsage()} calls will return false.
     * @return true if this counter is in the ended state after destroying
     */
    public boolean destroyForDecrypt() {
        return isEnded(AtomicCryptorUsageCounter::commenceDestroyForDecrypt);
    }

    /**
     * After this returns, future Decryptor and Encryptor usages can not be acquired.
     * @return true if this counter is in the ended state after destroying
     */
    public boolean destroyForBoth() {
        return isEnded(AtomicCryptorUsageCounter::commenceDestroyForBoth);
    }

    /** Combine two int reference counts into a single long */
    private static long combine(int encryptors, int decryptors) {
        return ((long) encryptors) << Integer.SIZE | 0xFFFFFFFFL & decryptors;
    }

    /** Extract the encryptor reference count from a long */
    private static int encryptorCount(long combined) {
        return (int) (combined >> Integer.SIZE);
    }

    /** Extract the decryptor reference count from a long */
    private static int decryptorCount(long combined) {
        return (int) combined;
    }

    private static long update(long combined, IntUnaryOperator encryptor, IntUnaryOperator decryptor) {
        final int encryptors = encryptorCount(combined);
        final int decryptors = decryptorCount(combined);
        int updatedEncryptors = encryptor.applyAsInt(encryptors);
        int updatedDecryptors = decryptor.applyAsInt(decryptors);
        return combine(updatedEncryptors, updatedDecryptors);
    }

    // a negative value indicates it has been destroyed
    private static int incrementCounterIfNotDestroyed(int counter) {
        return counter > 0 ? counter + 1 : counter;
    }

    // if the counter has been destroyed, it is negated, so we need to move towards the END state of -1
    private static int decrementCounter(int counter) {
        if (counter == 1 || counter == -1) {
            throw new IllegalStateException("cannot decrement at START or END, we have had more decrements than increments");
        }
        return counter > 0 ? counter - 1 : counter + 1;
    }

    // no-op if the counter has already been destroyed
    private static int destroyCounterIfNecessary(int counter) {
        return counter < 0 ? counter : -counter;
    }

    private static int identity(int value) {
        return value;
    }

    private boolean isEnded(LongUnaryOperator operator) {
        long updated = state.updateAndGet(operator);
        return updated == END;
    }

    /** Unary operator for acquiring an encryptor */
    private static long acquireEncryptorUsage(long combined) {
        return update(combined, AtomicCryptorUsageCounter::incrementCounterIfNotDestroyed, AtomicCryptorUsageCounter::identity);
    }

    /** Unary operator for acquiring a decryptor */
    private static long acquireDecryptorUsage(long combined) {
        return update(combined, AtomicCryptorUsageCounter::identity, AtomicCryptorUsageCounter::incrementCounterIfNotDestroyed);
    }

    /** Unary operator for releasing an encryptor */
    private static long releaseEncryptorUsage(long combined) {
        return update(combined, AtomicCryptorUsageCounter::decrementCounter, AtomicCryptorUsageCounter::identity);
    }

    /** Unary operator for releasing a decryptor */
    private static long releaseDecryptorUsage(long combined) {
        return update(combined, AtomicCryptorUsageCounter::identity, AtomicCryptorUsageCounter::decrementCounter);
    }

    /** Unary operator for "destroying" the key for encryption */
    private static long commenceDestroyForEncrypt(long combined) {
        return update(combined, AtomicCryptorUsageCounter::destroyCounterIfNecessary, AtomicCryptorUsageCounter::identity);
    }

    /** Unary operator for "destroying" the key for decryption */
    private static long commenceDestroyForDecrypt(long combined) {
        return update(combined, AtomicCryptorUsageCounter::identity, AtomicCryptorUsageCounter::destroyCounterIfNecessary);
    }

    /** Unary operator for "destroying" the key for both encryption and decryption */
    private static long commenceDestroyForBoth(long combined) {
        return update(combined, AtomicCryptorUsageCounter::destroyCounterIfNecessary, AtomicCryptorUsageCounter::destroyCounterIfNecessary);
    }

}
