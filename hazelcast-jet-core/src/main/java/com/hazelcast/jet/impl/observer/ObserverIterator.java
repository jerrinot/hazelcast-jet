package com.hazelcast.jet.impl.observer;

import com.hazelcast.jet.Observer;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public final class ObserverIterator<T> implements Iterator<T>, Observer<T> {
    private static final Object COMPLETED = new Object();

    private final BlockingQueue<Object> itemQueue;
    private boolean fullyDrained;
    private Object next;
    private volatile Throwable error;

    public ObserverIterator() {
        this.itemQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void onNext(@Nonnull T t) {
        itemQueue.add(t);
    }

    @Override
    public void onError(@Nonnull Throwable throwable) {
        error = throwable;
        itemQueue.add(COMPLETED);
    }

    @Override
    public void onComplete() {
        itemQueue.add(COMPLETED);
    }


    @Override
    public boolean hasNext() {
        if (fullyDrained) {
            return false;
        }
        if (next == null) {
            advance();
        }
        if (next != COMPLETED) {
            return true;
        }
        fullyDrained = true;
        if (error != null) {
            throw rethrow(error);
        }
        return false;
    }

    private void advance() {
        try {
            next = itemQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        }
    }

    @Override
    @Nonnull
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T item = (T) next;
        next = null;
        return item;
    }
}
