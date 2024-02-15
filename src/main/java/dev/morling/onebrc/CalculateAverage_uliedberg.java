/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Had a lof of fun with this! The preview Foreign Function & Memory API was nicer to work with than
 * ByteBuffer for sure.</br>
 *
 * On my machine (MacBook Pro, 2021, M1 Pro (10 cores), 32GB) on non-initial runs:
 * - baseline: ~3m 15s
 * - this version: ~4,5s
 * - thomaswue (top spot): ~1,3s
 */
public class CalculateAverage_uliedberg {
    private static final String FILE_PATH = "./measurements.txt";
    // 100 bytes for station name + max ";-99.9\n" for remaining
    private static final int MAX_ROW_SIZE = 107;

    public static void main(String[] args) {

        final File file = new File(FILE_PATH);
        final long fileSize = file.length();

        final int cores = Runtime.getRuntime().availableProcessors();
        // Last chunk size will be the remaining file size
        final int numberOfChunks = fileSize > (long) MAX_ROW_SIZE * cores ? cores : 1;
        final long defaultChunkSize = fileSize / numberOfChunks;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
                FileChannel fileChannel = raf.getChannel()) {

            final MemorySegment fileMemorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());

            var accumulatorStreams = IntStream.range(0, numberOfChunks)
                    .parallel()
                    .mapToObj(
                            chunkIndex -> {
                                var memorySegment = getSegmentForChunk(
                                        fileMemorySegment,
                                        chunkIndex,
                                        numberOfChunks,
                                        defaultChunkSize,
                                        fileSize);

                                return accumulateChunk(memorySegment);
                            });

            // Note: toConcurrentMap seems to be the same performance as toMap
            var accumulatorMap = accumulatorStreams
                    .flatMap(map -> map.entrySet().stream())
                    .collect(
                            Collectors.toConcurrentMap(
                                    Map.Entry::getKey, Map.Entry::getValue, MutableAccumulator::merge));

            // Note: parallel seems to be around the same performance, maybe different for 10k station input? Still small-ish
            var output = accumulatorMap.entrySet().stream()
                    .parallel()
                    .map(e -> Map.entry(convertToString(e.getKey()), e.getValue()))
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> STR."\{e.getKey()}=\{e.getValue().toStringPresentation()}")
                    .collect(Collectors.joining(", ", "{", "}"));

            System.out.println(output);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Re-implementation-ish of SharedUtils.toJavaStringInternal
    private static String convertToString(MemorySegmentWithHash segmentWithHash) {
        var segment = segmentWithHash.segment();
        var len = (int) segment.byteSize();
        byte[] bytes = new byte[len];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, bytes, 0, len);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // Return a MemorySegment with complete rows. I.e. no row is split between chunks.
    // Segment size will be defaultChunkSize + what's needed for a final complete row
    // Except for the last chunk, which will be the remaining file size
    private static MemorySegment getSegmentForChunk(
                                                    MemorySegment fileMemorySegment,
                                                    int chunkIndex,
                                                    int numberOfChunks,
                                                    long defaultChunkSize,
                                                    long fileSize) {

        IntPredicate isFirstChunk = n -> n == 0;
        IntPredicate isLastChunk = n -> n == numberOfChunks - 1;

        long chunkStart = chunkIndex * defaultChunkSize;
        long chunkSize = isLastChunk.test(chunkIndex)
                ? (fileSize - chunkStart)
                : (defaultChunkSize + CalculateAverage_uliedberg.MAX_ROW_SIZE);

        MemorySegment memorySegment = fileMemorySegment.asSlice(chunkStart, chunkSize);

        long position = 0;
        long segmentStartAfterFirstNewline = 0;
        if (!isFirstChunk.test(chunkIndex)) {
            // Get start position for first complete line
            while (memorySegment.get(ValueLayout.JAVA_BYTE, position++) != '\n') {
            }
            segmentStartAfterFirstNewline = position;
        }

        long segmentEndWithLastNewline = chunkSize;
        if (!isLastChunk.test(chunkIndex)) {
            position = chunkSize - CalculateAverage_uliedberg.MAX_ROW_SIZE;
            // Get end position for last complete line
            while (memorySegment.get(ValueLayout.JAVA_BYTE, position++) != '\n') {
            }
            segmentEndWithLastNewline = position;
        }

        return memorySegment.asSlice(
                segmentStartAfterFirstNewline, segmentEndWithLastNewline - segmentStartAfterFirstNewline);
    }

    private static Map<MemorySegmentWithHash, MutableAccumulator> accumulateChunk(MemorySegment segment) {
        Map<MemorySegmentWithHash, MutableAccumulator> chunkMap = new HashMap<>();

        long position = 0;
        while (position < segment.byteSize()) {
            var rowStart = position;
            var isStationPart = true;
            long s = 0;
            long v = 0;
            int stationHash = 1; // initial seed

            byte b;
            while ((b = segment.get(ValueLayout.JAVA_BYTE, position++)) != '\n') {
                if (b == ';') {
                    isStationPart = false;
                    continue;
                }
                if (isStationPart) {
                    s++;
                    stationHash = hashCode(b, stationHash);
                }
                else {
                    v++;
                }
            }
            var stationHigh = rowStart + s;
            var valueLow = stationHigh + 1; // skip ';'
            var valueHigh = valueLow + v;

            var measurement = Measurement.parse(
                    MemorySegmentWithHash.from(segment.asSlice(rowStart, stationHigh - rowStart), stationHash),
                    segment.asSlice(valueLow, valueHigh - valueLow));

            chunkMap.compute(
                    measurement.station(),
                    (k, acc) -> (acc == null) ? MutableAccumulator.from(measurement) : acc.add(measurement));
        }

        return chunkMap;
    }

    private static int hashCode(byte b, int initialValue) {
        return 31 * initialValue + (int) b;
    }

    public record MemorySegmentWithHash(MemorySegment segment, int hash) {

        public static MemorySegmentWithHash from(MemorySegment segment, int hash) {
            return new MemorySegmentWithHash(segment, hash);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        // Note: speeding up this would gain a lot.
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MemorySegmentWithHash that = (MemorySegmentWithHash) o;
            return this.hash == that.hash && this.segment.asByteBuffer().equals(that.segment.asByteBuffer());
        }
    }

    public record Measurement(MemorySegmentWithHash station, int value) {

        public static Measurement parse(MemorySegmentWithHash stationData, MemorySegment valueData) {
            var value = parseValue(valueData);
            return new Measurement(stationData, value);
        }

        static int parseValue(MemorySegment segment) {
            var isNegative = segment.get(ValueLayout.JAVA_BYTE, 0) == '-';
            var sign = isNegative ? -1 : 1;
            var size = segment.byteSize();
            var hasTenDigit = (size - (isNegative ? 1 : 0) - 1) == 3;

            IntUnaryOperator digitAt =
                    (fromEnd) -> segment.get(ValueLayout.JAVA_BYTE, size - fromEnd - 1) - '0';

            return sign
                    * ((hasTenDigit ? digitAt.applyAsInt(3) : 0) * 100
                    + digitAt.applyAsInt(2) * 10
                    // skip the decimal point at 1
                    + digitAt.applyAsInt(0));
        }

        // For debugging
        @Override
        public String toString() {
            return STR."{station: \{convertToString(station)}, station hash: \{station.hash}, value: \{value}}";
        }
    }

    public static class MutableAccumulator {
        public int min;
        public int max;
        public int sum;
        public int count;

        private MutableAccumulator() {
        }

        private MutableAccumulator(int min, int max, int sum, int count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public static MutableAccumulator from(Measurement m) {
            return new MutableAccumulator(m.value(), m.value(), m.value(), 1);
        }

        public MutableAccumulator add(Measurement m) {
            this.min = Math.min(min, m.value());
            this.max = Math.max(max, m.value());
            this.sum += m.value();
            this.count++;
            return this;
        }

        // Merge in place
        public MutableAccumulator merge(MutableAccumulator other) {
            this.min = Math.min(min, other.min);
            this.max = Math.max(max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }

        // Final presentation, int -> double -> String
        public String toStringPresentation() {
            var minP = (double) min / 10;
            var maxP = (double) max / 10;
            var meanP = round((double) sum / 10 / count);
            return STR."\{minP}/\{meanP}/\{maxP}";
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        // For debugging
        @Override
        public String toString() {
            return STR."{min: \{min}, max: \{max}, sum: \{sum}, count: \{count}}";
        }
    }
}
