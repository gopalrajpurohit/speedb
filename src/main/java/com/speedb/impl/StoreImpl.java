package com.speedb.impl;

import com.speedb.api.Configuration;
import com.speedb.api.StoreReader;
import com.speedb.api.StoreWriter;
import com.speedb.utils.TempUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Static implementation factory.
 */
public final class StoreImpl {

    private final static Logger LOGGER = Logger.getLogger(StoreImpl.class.getName());

    private StoreImpl() {
    }

    public static StoreReader<byte[], byte[]> createReader(File file, Configuration config) {
        if (file == null || config == null) {
            throw new NullPointerException();
        }
        LOGGER.log(Level.INFO, "Initialize reader from file {0}", file.getName());
        return new ByteArrayStoreReaderImpl(config, file);
    }

    public static StoreReader<byte[], byte[]> createReader(InputStream stream, Configuration config) {
        if (stream == null || config == null) {
            throw new NullPointerException();
        }
        LOGGER.log(Level.INFO, "Initialize reader from stream, copying into temp folder");
        try {
            File file = TempUtils.copyIntoTempFile("speedbtempwriterdir", stream);
            LOGGER.log(Level.INFO, "Copied stream into temp file {0}", file.getName());
            return new ByteArrayStoreReaderImpl(config, file);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static StoreWriter<byte[], byte[]> createWriter(File file, Configuration config) {
        if (file == null || config == null) {
            throw new NullPointerException();
        }
        try {
            LOGGER.log(Level.INFO, "Initialize writer from file {0}", file.getName());
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                if (parent.mkdirs()) {
                    LOGGER.log(Level.INFO, "Creating directories for path {0}", file.getName());
                } else {
                    throw new RuntimeException(String.format("Couldn't create directory %s", parent));
                }
            }
            return new ByteArrayStoreWriterImpl(config, file);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static StoreWriter<byte[], byte[]> createWriter(OutputStream stream, Configuration config) {
        if (stream == null || config == null) {
            throw new NullPointerException();
        }
        LOGGER.info("Initialize writer from stream");
        return new ByteArrayStoreWriterImpl(config, stream);
    }
}
