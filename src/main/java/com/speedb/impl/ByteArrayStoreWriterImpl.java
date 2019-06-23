package com.speedb.impl;

import com.speedb.api.ByteArrayStoreWriter;
import com.speedb.api.Configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Store writer implementation.
 */
final class ByteArrayStoreWriterImpl implements ByteArrayStoreWriter {
    // Logger
    private final static Logger LOGGER = Logger.getLogger(ByteArrayStoreWriterImpl.class.getName());
    // Configuration
    private final Configuration config;
    // Storage
    private final StorageWriter storage;
    // File (can be null)
    private final File file;
    // Stream
    private final OutputStream outputStream;
    // Opened?
    private boolean opened;

    /**
     * File constructor.
     *
     * @param config configuration
     * @param file   input file
     */
    ByteArrayStoreWriterImpl(Configuration config, File file)
            throws IOException {
        this(config, new FileOutputStream(file), file);
    }

    /**
     * Stream constructor.
     *
     * @param config configuration
     * @param stream input stream
     */
    ByteArrayStoreWriterImpl(Configuration config, OutputStream stream) {
        this(config, stream, null);
    }

    /**
     * Private constructor.
     *
     * @param config configuration
     * @param stream output stream
     */
    private ByteArrayStoreWriterImpl(Configuration config, OutputStream stream, File file) {
        this.config = config;
        this.outputStream = stream;
        this.file = file;

        // Open storage
        LOGGER.log(Level.INFO, "Opening writer storage");
        storage = new StorageWriter(config, outputStream);
        opened = true;
    }

    @Override
    public void close() {
        checkOpen();
        try {
            if (file != null) {
                LOGGER.log(Level.INFO, "Closing writer storage, writing to file at " + file.getAbsolutePath());
            } else {
                LOGGER.log(Level.INFO, "Closing writer storage, writing to stream");
            }

            storage.close();
            outputStream.close();
            opened = false;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public void put(byte[] key, byte[] value) {
        checkOpen();
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        try {
            storage.put(key, value);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    // UTILITIES
    private void checkOpen() {
        if (!opened) {
            throw new IllegalStateException("The store is closed");
        }
    }
}
