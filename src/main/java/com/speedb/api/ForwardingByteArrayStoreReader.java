package com.speedb.api;

public class ForwardingByteArrayStoreReader
        extends ForwardingStoreReader<byte[], byte[]>
        implements ByteArrayStoreReader {
    public ForwardingByteArrayStoreReader(StoreReader<byte[], byte[]> delegate) {
        super(delegate);
    }
}
