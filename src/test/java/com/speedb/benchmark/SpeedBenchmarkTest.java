package com.speedb.benchmark;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.speedb.api.*;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.function.Function;

import static org.junit.Assert.*;

public class SpeedBenchmarkTest {

    private byte[] key(long recordId, long numRecords) {
        HashFunction md5 = Hashing.murmur3_128();

        String keyStr = md5.hashLong(recordId).toString();

        if ((recordId % (numRecords / 10)) == 0) {
            System.out.println(String.format("key = %s", keyStr));
        }

        return keyStr.getBytes();
    }

    private byte[] value(long recordId, long numRecords) {
        HashFunction md5 = Hashing.murmur3_128();

        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 1; i <= 512; i++) {
            stringBuilder = stringBuilder
                    .append(md5.hashLong(recordId + i).toString());
        }
        String valueStr = stringBuilder.toString();

        if ((recordId % (numRecords / 10)) == 0) {
            System.out.println(String.format("value = %s", valueStr));
        }

        return valueStr.getBytes();
    }

    private Function<Void, Void> write(Configuration conf, long numRecords) {
        ByteArrayStoreWriter writer = new ForwardingByteArrayStoreWriter(
                SpeedB.createWriter(
                        new File("target/speedb.benchmark.sdb"), conf));

        long startTimeWritens = System.nanoTime();
        long writeLocalPutNs = 0;
        for (long recordId = 0; recordId < numRecords; recordId++) {
            byte[] key = key(recordId, numRecords);
            byte[] value = value(recordId, numRecords);

            long startLocalPutNs = System.nanoTime();
            writer.put(key, value);
            long endLocalPutNs = System.nanoTime();

            writeLocalPutNs += (endLocalPutNs - startLocalPutNs);
        }

        long startLocalWriteNs = System.nanoTime();
        writer.close();
        long endLocalWriteNs = System.nanoTime();
        long endTimeWritens = System.nanoTime();

        final long totalWriteNs = (endTimeWritens - startTimeWritens);
        final long writeCloseNs = (endLocalWriteNs - startLocalWriteNs);
        final long writePutNs = writeLocalPutNs;

        System.out.flush();
        System.err.flush();

        return new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                System.out.println(String.format("Total Write Time : %d ms", totalWriteNs / 1000000));
                System.out.println(String.format("Put Time : %d ms", (writePutNs) / 1000000));
                System.out.println(String.format("Write Time : %d ms", (writeCloseNs) / 1000000));

                return null;
            }
        };
    }

    private Function<Void, Void> verifySeq(Configuration conf, long numRecords, boolean verify) {
        ByteArrayStoreReader reader = new ForwardingByteArrayStoreReader(
                SpeedB.createReader(
                        new File("target/speedb.benchmark.sdb"), conf));

        final long startTimens = System.nanoTime();
        long localLookupTimens = 0;

        Random random = new Random();
        for (long recordId = 0; recordId < numRecords; recordId++) {
            byte[] key = key(recordId, numRecords);

            long startLocalns = System.nanoTime();
            byte[] resultValue = reader.get(key);
            long endLocalns = System.nanoTime();
            localLookupTimens += (endLocalns - startLocalns);

            assertNotNull(resultValue);
            if (verify && (random.nextInt(100) == 0)) {
                byte[] value = value(recordId, numRecords);
                assertArrayEquals(value, resultValue);
            }
        }
        final long endTimens = System.nanoTime();

        reader.close();
        System.out.flush();
        System.err.flush();

        final long lookupTimens = localLookupTimens;

        return new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                System.out.println(String.format("SeqRead: Total Time : %d ms", (endTimens - startTimens) / 1000000));
                System.out.println(String.format("SeqRead: Lookup Time : %d ms", (lookupTimens) / 1000000));
                return null;
            }
        };
    }

    private Function<Void, Void> verifyRepeatedSeq(Configuration conf, long numRecords, boolean verify) {
        ByteArrayStoreReader reader = new ForwardingByteArrayStoreReader(
                SpeedB.createReader(
                        new File("target/speedb.benchmark.sdb"), conf));

        final long startTimens = System.nanoTime();
        long localLookupTimens = 0;

        Random random = new Random();
        for (long recordId = 0; recordId < numRecords; recordId++) {
            double sqrtOfRecordId = Math.sqrt(recordId);
            double sqrtOfSqrtOfRecordId = Math.sqrt(sqrtOfRecordId);
            long nextRecordId =  (int) (sqrtOfRecordId * sqrtOfSqrtOfRecordId);

            byte[] key = key(nextRecordId, numRecords);

            long startLocalns = System.nanoTime();
            byte[] resultValue = reader.get(key);
            long endLocalns = System.nanoTime();
            localLookupTimens += (endLocalns - startLocalns);

            assertNotNull(resultValue);
            if (verify && (random.nextInt(100) == 0)) {
                byte[] value = value(nextRecordId, numRecords);
                assertArrayEquals(value, resultValue);
            }
        }
        final long endTimens = System.nanoTime();

        reader.close();
        System.out.flush();
        System.err.flush();

        final long lookupTimens = localLookupTimens;

        return new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                System.out.println(String.format("SeqRepeatedRead: Total Time : %d ms", (endTimens - startTimens) / 1000000));
                System.out.println(String.format("SeqRepeatedRead: Lookup Time : %d ms", (lookupTimens) / 1000000));
                return null;
            }
        };
    }

    private Function<Void, Void> verifyRepeatedRandom(Configuration conf, long numRecords, boolean verify) {
        ByteArrayStoreReader reader = new ForwardingByteArrayStoreReader(
                SpeedB.createReader(
                        new File("target/speedb.benchmark.sdb"), conf));

        Random random = new Random(System.currentTimeMillis());

        final long startTimens = System.nanoTime();
        long localLookupTimens = 0;
        for (long recordId = 0; recordId < numRecords; recordId++) {
            double sqrtOfRecordId = Math.sqrt(random.nextInt((int) numRecords));
            double sqrtOfSqrtOfRecordId = Math.sqrt(Math.sqrt(random.nextInt((int) numRecords)));
            long randomRecordId =  (int) (sqrtOfRecordId * sqrtOfSqrtOfRecordId);
            byte[] key = key(randomRecordId, numRecords);

            long startLocalns = System.nanoTime();
            byte[] resultValue = reader.get(key);
            long endLocalns = System.nanoTime();
            localLookupTimens += (endLocalns - startLocalns);

            assertNotNull(resultValue);
            if (verify && (random.nextInt(100) == 0)) {
                byte[] value = value(randomRecordId, numRecords);
                assertArrayEquals(value, resultValue);
            }
        }
        final long endTimens = System.nanoTime();

        reader.close();
        System.out.flush();
        System.err.flush();

        final long lookupTimens = localLookupTimens;

        return new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                System.out.println(String.format("RepeatedRandom: Total Time : %d ms", (endTimens - startTimens) / 1000000));
                System.out.println(String.format("RepeatedRandom: Lookup Time : %d ms", (lookupTimens) / 1000000));
                return null;
            }
        };
    }

    private Function<Void, Void> verifyRandom(Configuration conf, long numRecords, boolean verify) {
        ByteArrayStoreReader reader = new ForwardingByteArrayStoreReader(
                SpeedB.createReader(
                        new File("target/speedb.benchmark.sdb"), conf));

        Random random = new Random(System.currentTimeMillis());

        final long startTimens = System.nanoTime();
        long localLookupTimens = 0;
        for (long recordId = 0; recordId < numRecords; recordId++) {
            long randomRecordId = random.nextInt((int) numRecords);
            byte[] key = key(randomRecordId, numRecords);

            long startLocalns = System.nanoTime();
            byte[] resultValue = reader.get(key);
            long endLocalns = System.nanoTime();
            localLookupTimens += (endLocalns - startLocalns);

            assertNotNull(resultValue);
            if (verify && (random.nextInt(100) == 0)) {
                byte[] value = value(randomRecordId, numRecords);
                assertArrayEquals(value, resultValue);
            }
        }
        final long endTimens = System.nanoTime();

        reader.close();
        System.out.flush();
        System.err.flush();

        final long lookupTimens = localLookupTimens;

        return new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                System.out.println(String.format("Random: Total Time : %d ms", (endTimens - startTimens) / 1000000));
                System.out.println(String.format("Random: Lookup Time : %d ms", (lookupTimens) / 1000000));
                return null;
            }
        };
    }

    private Function<Void, Void> verifySeqAbsent(Configuration conf, long numRecords) {
        ByteArrayStoreReader reader = new ForwardingByteArrayStoreReader(
                SpeedB.createReader(
                        new File("target/speedb.benchmark.sdb"), conf));

        final long startTimens = System.nanoTime();
        long localLookupTimens = 0;
        for (long recordId = 0; recordId < numRecords; recordId++) {
            byte[] key = key(recordId + numRecords, numRecords);

            long startLocalns = System.nanoTime();
            byte[] resultValue = reader.get(key);
            long endLocalns = System.nanoTime();
            localLookupTimens += (endLocalns - startLocalns);
            assertNull(resultValue);
        }
        final long endTimens = System.nanoTime();

        reader.close();
        System.out.flush();
        System.err.flush();

        final long lookupTimens = localLookupTimens;

        return new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                System.out.println(String.format("SeqAbsent: Total Time : %d ms", (endTimens - startTimens) / 1000000));
                System.out.println(String.format("SeqAbsent: Lookup Time : %d ms", (lookupTimens) / 1000000));
                return null;
            }
        };
    }

    private Function<Void, Void> verifyRandomAbsent(Configuration conf, long numRecords) {
        ByteArrayStoreReader reader = new ForwardingByteArrayStoreReader(
                SpeedB.createReader(
                        new File("target/speedb.benchmark.sdb"), conf));

        Random random = new Random(System.currentTimeMillis());

        final long startTimens = System.nanoTime();
        long localLookupTimens = 0;
        for (long recordId = 0; recordId < numRecords; recordId++) {
            long randomRecordId = random.nextInt((int) numRecords) + numRecords;
            byte[] key = key(randomRecordId, numRecords);

            long startLocalns = System.nanoTime();
            byte[] resultValue = reader.get(key);
            long endLocalns = System.nanoTime();
            localLookupTimens += (endLocalns - startLocalns);
            assertNull(resultValue);
        }
        final long endTimens = System.nanoTime();

        reader.close();
        System.out.flush();
        System.err.flush();

        final long lookupTimens = localLookupTimens;

        return new Function<Void, Void>() {
            @Override
            public Void apply(Void aVoid) {
                System.out.println(String.format("RandomAbsent: Total Time : %d ms", (endTimens - startTimens) / 1000000));
                System.out.println(String.format("RandomAbsent: Lookup Time : %d ms", (lookupTimens) / 1000000));
                return null;
            }
        };
    }

    @Test
    public void testBenchmark() {
        Configuration conf = SpeedB.newConfiguration();

        conf.set(Configuration.MMAP_DATA_ENABLED, "false");
        conf.set(Configuration.MMAP_SEGMENT_SIZE, "0");
        conf.set(Configuration.LOAD_FACTOR, "0.2");
        conf.set(Configuration.CACHE_ENABLED, "false");
        conf.set(Configuration.COMPRESSION_ENABLED, "false");
        long numRecords = 1000000;

        //Function<Void, Void> w = write(conf, numRecords);
        Function<Void, Void> vs = verifySeq(conf, numRecords, false);
        Function<Void, Void> vrs = verifyRepeatedSeq(conf, numRecords, false);
        Function<Void, Void> vsa = verifySeqAbsent(conf, numRecords);
        Function<Void, Void> vrr = verifyRepeatedRandom(conf, numRecords, false);
        //Function<Void, Void> vr = verifyRandom(conf, numRecords, false);
        Function<Void, Void> vra = verifyRandomAbsent(conf, numRecords);

        //w.apply(null);
        vs.apply(null);
        vrs.apply(null);
        vsa.apply(null);
        vrr.apply(null);
        //vr.apply(null);
        vra.apply(null);
    }
}
