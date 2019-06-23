package com.speedb.api;

public class ForwardingByteArrayStoreWriter
        extends ForwardingStoreWriter<byte[], byte[]>
        implements ByteArrayStoreWriter {
    public ForwardingByteArrayStoreWriter(StoreWriter<byte[], byte[]> delegate) {
        super(delegate);
    }
}
