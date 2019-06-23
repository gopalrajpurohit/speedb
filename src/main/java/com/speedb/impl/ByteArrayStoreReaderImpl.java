package com.speedb.impl;

import com.speedb.api.ByteArrayStoreReader;
import com.speedb.api.Configuration;
import com.speedb.utils.DataInputOutput;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Store reader implementation.
 */
final class ByteArrayStoreReaderImpl implements ByteArrayStoreReader {

    // Logger
    private final static Logger LOGGER = Logger.getLogger(ByteArrayStoreReaderImpl.class.getName());
    // Configuration
    private final Configuration config;
    // Buffer
    private final DataInputOutput dataInputOutput = new DataInputOutput();
    // Storage
    private final StorageReader storage;
    // File
    private final File file;
    // Opened?
    private boolean opened;

    /**
     * Private constructor.
     *
     * @param config configuration
     * @param file   store file
     */
    ByteArrayStoreReaderImpl(Configuration config, File file) {
        this.config = config;
        this.file = file;

        // Open storage
        try {
            LOGGER.log(Level.INFO, "Opening reader storage");
            storage = new StorageReader(config, file);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        opened = true;
    }

    @Override
    public byte[] get(byte[] key) {
        checkOpen();
        if (key == null) {
            throw new NullPointerException("The key can't be null");
        }
        try {
            byte[] valueBytes = storage.get(key);
            return valueBytes;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        checkOpen();
        try {
            LOGGER.log(Level.INFO, "Closing reader storage");
            storage.close();
            opened = false;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Checks if the store is open and throws an exception otherwise.
     */
    private void checkOpen() {
        if (!opened) {
            throw new IllegalStateException("The store is closed");
        }
    }
}
