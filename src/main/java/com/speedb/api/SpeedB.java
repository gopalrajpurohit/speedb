package com.speedb.api;

import com.speedb.impl.StoreImpl;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Factory for store readers and writers.
 * <p>
 * This class is the entry point to obtain {@link StoreReader} and
 * {@link StoreWriter} interfaces.
 */
public final class SpeedB {

    /**
     * This class is only static
     */
    private SpeedB() {
    }

    /**
     * Creates a store reader from the specified <code>file</code> with a default configuration.
     * <p>
     * The file must exists.
     *
     * @param file a SpeedB store file
     * @return a store reader
     */
    public static StoreReader<byte[], byte[]> createReader(File file) {
        return StoreImpl.createReader(file, newConfiguration());
    }

    /**
     * Creates a store reader from the specified <code>file</code>.
     * <p>
     * The file must exists.
     *
     * @param file   a SpeedB store file
     * @param config configuration
     * @return a store reader
     */
    public static StoreReader<byte[], byte[]> createReader(File file, Configuration config) {
        return StoreImpl.createReader(file, config);
    }

    /**
     * Creates a store reader from the specified <code>stream</code>.
     * <p>
     * The reader will read the stream and write its content to a temporary file when this method is called. This is
     * specifically suited for stream coming from the JAR as a resource. The stream will be closed by this method.
     *
     * @param stream an input stream on a SpeedB store file
     * @param config configuration
     * @return a store reader
     */
    public static StoreReader<byte[], byte[]> createReader(InputStream stream, Configuration config) {
        return StoreImpl.createReader(stream, config);
    }

    /**
     * Creates a store writer with the specified <code>file</code> as destination with a default configuration.
     * <p>
     * The parent folder is created if missing.
     *
     * @param file location of the output file
     * @return a store writer
     */
    public static StoreWriter<byte[], byte[]> createWriter(File file) {
        return StoreImpl.createWriter(file, newConfiguration());
    }

    /**
     * Creates a store writer with the specified <code>file</code> as destination.
     * <p>
     * The parent folder is created if missing.
     *
     * @param file   location of the output file
     * @param config configuration
     * @return a store writer
     */
    public static StoreWriter<byte[], byte[]> createWriter(File file, Configuration config) {
        return StoreImpl.createWriter(file, config);
    }

    /**
     * Creates a store writer with the specified  <code>stream</code> as destination.
     * <p>
     * The writer will only write bytes to the stream when {@link StoreWriter#close() }
     * is called.
     *
     * @param stream output stream
     * @param config configuration
     * @return a store writer
     */
    public static StoreWriter<byte[], byte[]> createWriter(OutputStream stream, Configuration config) {
        return StoreImpl.createWriter(stream, config);
    }

    /**
     * Creates new configuration with default values.
     *
     * @return new configuration
     */
    public static Configuration newConfiguration() {
        return new Configuration();
    }
}
