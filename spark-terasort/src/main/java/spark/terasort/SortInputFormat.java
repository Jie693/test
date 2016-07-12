/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package spark.terasort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.StringUtils;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SortInputFormat extends FileInputFormat<byte[], byte[]> {

    static final String PARTITION_FILENAME = "_partition.lst";
    static final int KEY_LENGTH = 10;
    static final int VALUE_LENGTH = 90;
    static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH;


    @Override
    public RecordReader<byte[], byte[]>
    createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new TeraRecordReader();
    }


    static class TextSampler implements IndexedSortable {
        private ArrayList<byte[]> records = new ArrayList<byte[]>();

        public int compare(int i, int j) {
            byte[] left = records.get(i);
            byte[] right = records.get(j);
            return WritableComparator.compareBytes(left, 0, left.length,
                    right, 0, right.length);
        }

        public void swap(int i, int j) {
            byte[] left = records.get(i);
            byte[] right = records.get(j);
            records.set(j, left);
            records.set(i, right);
        }

        public void addKey(byte[] key) {
            synchronized (this) {
                records.add(key);
            }
        }

        /**
         * Find the split points for a given sample. The sample keys are sorted
         * and down sampled to find even split points for the partitions. The
         * returned keys should be the start of their respective partitions.
         *
         * @param numPartitions the desired number of partitions
         * @return an array of size numPartitions - 1 that holds the split points
         */
        byte[][] createPartitions(int numPartitions) {
            int numRecords = records.size();
            System.out.println("Making " + numPartitions + " from " + numRecords +
                    " sampled records");
            if (numPartitions > numRecords) {
                throw new IllegalArgumentException
                        ("Requested more partitions than input keys (" + numPartitions +
                                " > " + numRecords + ")");
            }
            new QuickSort().sort(this, 0, records.size());
            float stepSize = numRecords / (float) numPartitions;
            byte[][] result = new byte[numPartitions - 1][];
            for (int i = 1; i < numPartitions; ++i) {
                result[i - 1] = records.get(Math.round(stepSize * i));
            }
            return result;
        }
    }



    /**
     * By default reads 100,000 keys from 10 locations in the input, sorts
     * them and picks N-1 keys to generate N equally sized partitions.
     *
     * @param job
     * @param partitionNum
     * @param sampleNum
     * @param partFile
     * @param threadNum
     * @throws Throwable
     */
    public static void writePartitionFile(final JobContext job,
                                          int partitionNum,
                                          long sampleNum,
                                          Path partFile,
                                          int threadNum
    ) throws Throwable {
        long t1 = System.currentTimeMillis();
        Configuration conf = job.getConfiguration();
        final SortInputFormat inFormat = new SortInputFormat();
        final TextSampler sampler = new TextSampler();
        int partitions = partitionNum;
        long sampleSize = sampleNum > 0 ? sampleNum : 100000;
        final List<InputSplit> splits = inFormat.getSplits(job);
        long t2 = System.currentTimeMillis();
        System.out.println("Computing input splits took " + (t2 - t1) + "ms");
        final int samples = Math.min(threadNum > 0 ? threadNum : 1000, splits.size());
        System.out.println("Sampling " + samples + " splits of " + splits.size());
        final long recordsPerSample = sampleSize / samples;
        final int sampleStep = splits.size() / samples;
        int threadsNum = (samples % 2 == 0) ? (samples / 2) : (samples / 2 + 1);
        Thread[] samplerReader = new Thread[samples];
        SamplerThreadGroup threadGroup = new SamplerThreadGroup("Sampler Reader Thread Group");
        // take N samples from different parts of the input
        for (int i = 0; i < threadsNum; ++i) {
            final int idx = i;
            samplerReader[i] =
                    new Thread(threadGroup, "Sampler Reader " + idx) {
                        {
                            setDaemon(true);
                        }

                        public void run() {
                            long records = 0;
                            try {
                                TaskAttemptContext context = new TaskAttemptContextImpl(
                                        job.getConfiguration(), new TaskAttemptID());
                                RecordReader<byte[], byte[]> reader =
                                        inFormat.createRecordReader(splits.get(sampleStep * idx * 2),
                                                context);
                                reader.initialize(splits.get(sampleStep * idx), context);
                                while (reader.nextKeyValue()) {
                                    byte[] newKey = new byte[KEY_LENGTH];
                                    System.arraycopy(reader.getCurrentKey(), 0, newKey, 0, KEY_LENGTH);
                                    sampler.addKey(newKey);
                                    records += 1;
                                    if (recordsPerSample <= records) {
                                        break;
                                    }
                                }

                                if (idx * 2 + 1 < samples) {
                                    records = 0;
                                    reader =
                                            inFormat.createRecordReader(splits.get(sampleStep * (idx * 2 + 1)),
                                                    context);
                                    reader.initialize(splits.get(sampleStep * idx), context);
                                    while (reader.nextKeyValue()) {
                                        byte[] newKey = new byte[KEY_LENGTH];
                                        System.arraycopy(reader.getCurrentKey(), 0, newKey, 0, KEY_LENGTH);
                                        sampler.addKey(newKey);
                                        records += 1;
                                        if (recordsPerSample <= records) {
                                            break;
                                        }
                                    }
                                }


                            } catch (IOException ie) {
                                System.err.println("Got an exception while reading splits " +
                                        StringUtils.stringifyException(ie));
                                throw new RuntimeException(ie);
                            } catch (InterruptedException e) {

                            }
                        }
                    };
            samplerReader[i].start();
        }
        FileSystem outFs = partFile.getFileSystem(conf);
        DataOutputStream writer = outFs.create(partFile, true, 64 * 1024, (short) 10,
                outFs.getDefaultBlockSize(partFile));
        for (int i = 0; i < threadsNum; i++) {
            try {
                samplerReader[i].join();
                if (threadGroup.getThrowable() != null) {
                    throw threadGroup.getThrowable();
                }
            } catch (InterruptedException e) {
            }
        }

        for (byte[] split : sampler.createPartitions(partitions)) {
            WritableUtils.writeVInt(writer, split.length);
            writer.write(split, 0, split.length);
        }
        writer.close();
        long t3 = System.currentTimeMillis();
        System.out.println("Sampling took " + (t3 - t2) + "ms");
    }


    static class SamplerThreadGroup extends ThreadGroup {

        private Throwable throwable;

        public SamplerThreadGroup(String s) {
            super(s);
        }

        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            this.throwable = throwable;
        }

        public Throwable getThrowable() {
            return this.throwable;
        }

    }

    static class TeraRecordReader extends RecordReader<byte[], byte[]> {
        private FSDataInputStream in;
        private long offset;
        private long length;
        private byte[] buffer = new byte[RECORD_LENGTH];
        private byte[] key;
        private byte[] value;

        public TeraRecordReader() throws IOException {
        }


        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            Path p = fileSplit.getPath();
            FileSystem fs = p.getFileSystem(context.getConfiguration());
            in = fs.open(p);
            long start = fileSplit.getStart();
            // find the offset to start at a record boundary
            offset = (RECORD_LENGTH - (start % RECORD_LENGTH)) % RECORD_LENGTH;
            in.seek(start + offset);
            length = fileSplit.getLength();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public byte[] getCurrentKey() {
            return key;
        }

        @Override
        public byte[] getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() throws IOException {
            return (float) offset / length;
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (offset >= length) {
                return false;
            }
            int read = 0;
            while (read < RECORD_LENGTH) {
                long newRead = in.read(buffer, read, RECORD_LENGTH - read);
                if (newRead == -1) {
                    if (read == 0) {
                        return false;
                    } else {
                        throw new EOFException("read past eof");
                    }
                }
                read += newRead;
            }
            if (key == null) {
                key = new byte[KEY_LENGTH];
            }
            if (value == null) {
                value = new byte[VALUE_LENGTH];
            }
            System.arraycopy(buffer, 0, key, 0, KEY_LENGTH);
            System.arraycopy(buffer, KEY_LENGTH, value, 0, VALUE_LENGTH);
            offset += RECORD_LENGTH;
            return true;
        }
    }


}


