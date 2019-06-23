package com.speedb.api;

public class ForwardingStoreWriter<K, V> implements StoreWriter<K, V> {
    private final StoreWriter<K, V> delegate;

    public ForwardingStoreWriter(StoreWriter<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public Configuration getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public void put(K key, V value) {
        delegate.put(key, value);
    }
}
