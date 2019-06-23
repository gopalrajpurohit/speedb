package com.speedb.api;

public class ForwardingStoreReader<K, V> implements StoreReader<K, V> {
    private StoreReader<K, V> delegate;

    public ForwardingStoreReader(StoreReader<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public V get(K key) {
        return delegate.get(key);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
