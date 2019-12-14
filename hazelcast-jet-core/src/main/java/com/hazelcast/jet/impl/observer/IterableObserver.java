package com.hazelcast.jet.impl.observer;

import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.concurrent.MPSCQueue;
import com.hazelcast.jet.Observer;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class IterableObserver<T> implements Iterable<T>, Observer<T> {
    //TODO: Blocking impl.
    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, SECONDS.toNanos(1), SECONDS.toNanos(10));

    private final Queue<T> itemQueue;
    private volatile boolean completed;
    private volatile Throwable error;

    public IterableObserver() {
        this.itemQueue = new MPSCQueue<>(IDLER);
    }

    @Override
    public void onNext(@Nonnull T t) {
        itemQueue.add(t);
    }

    @Override
    public void onError(@Nonnull Throwable throwable) {
        error = throwable;
        completed = true;
    }

    @Override
    public void onComplete() {
        completed = true;
    }

    public boolean isCompleted() {
        return completed;
    }

    @Override
    public Iterator<T> iterator() {
        return new BlockingIterator();
    }

    private class BlockingIterator implements Iterator<T> {
        @Override
        public boolean hasNext() {
            for (int i = 0;; i++) {
                if (!itemQueue.isEmpty()) {
                    return true;
                }
                if (isCompleted()) {
                    return !itemQueue.isEmpty();
                }
                IDLER.idle(i);
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw error == null ? new NoSuchElementException() : rethrow(error);
            }
            return itemQueue.poll();
        }
    }
}
