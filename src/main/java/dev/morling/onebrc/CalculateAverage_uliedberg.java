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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CalculateAverage_uliedberg {
    private static final String FILE_PATH = "./measurements.txt";

    public static void main(String[] args) {

        final File file = new File(FILE_PATH);
        final long fileSize = file.length();

        // 100 bytes for station name + max ";-99.9\n" for remaining
        final int MAX_ROW_SIZE = 107;

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
                                        fileSize,
                                        MAX_ROW_SIZE);

                                return accumulateChunk(memorySegment);
                            });

            // TODO: reduce more efficiently?
            // Note: toConcurrentMap seems to be the same performance as toMap
            var accumulatorMap = accumulatorStreams
                    .flatMap(map -> map.entrySet().stream())
                    .collect(
                            Collectors.toConcurrentMap(
                                    Map.Entry::getKey, Map.Entry::getValue, MutableAccumulator::merge));

            // TODO: sort could be better? Maybe only use String in te printing? Same for the value though...
            var outputMap = accumulatorMap.entrySet().stream()
                    .collect(
                            Collectors.toMap(
                                    e -> convertToString(e.getKey()),
                                    e -> e.getValue().toStringPresentation(),
                                    (v1, v2) -> {
                                        throw new RuntimeException("No duplicates expected");
                                    },
                                    TreeMap::new));

            System.out.println(outputMap);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: shouldn't copy the bytes, will be done in string as well
    private static String convertToString(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
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
                                                    long fileSize,
                                                    int maxRowSize) {

        IntPredicate isFirstChunk = n -> n == 0;
        IntPredicate isLastChunk = n -> n == numberOfChunks - 1;

        long chunkStart = chunkIndex * defaultChunkSize;
        long chunkSize = isLastChunk.test(chunkIndex) ? (fileSize - chunkStart) : (defaultChunkSize + maxRowSize);

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
            position = chunkSize - maxRowSize;
            // Get end position for last complete line
            while (memorySegment.get(ValueLayout.JAVA_BYTE, position++) != '\n') {
            }
            segmentEndWithLastNewline = position;
        }

        return memorySegment.asSlice(
                segmentStartAfterFirstNewline, segmentEndWithLastNewline - segmentStartAfterFirstNewline);
    }

    // TODO: possible to pre-calc hashcode for the station in the row loop
    private static Map<ByteBuffer, MutableAccumulator> accumulateChunk(MemorySegment segment) {
        Map<ByteBuffer, MutableAccumulator> chunkMap = new HashMap<>();

        long position = 0;
        while (position < segment.byteSize()) {
            var rowStart = position;
            var isStationPart = true;
            long s = 0;
            long v = 0;

            byte b;
            while ((b = segment.get(ValueLayout.JAVA_BYTE, position++)) != '\n') {
                if (b == ';') {
                    isStationPart = false;
                    continue;
                }
                if (isStationPart) {
                    s++;
                }
                else {
                    v++;
                }
            }
            var stationHigh = rowStart + s;
            var valueLow = stationHigh + 1; // skip ';'
            var valueHigh = valueLow + v;

            var measurement = Measurement.parse(
                    segment.asSlice(rowStart, stationHigh - rowStart),
                    segment.asSlice(valueLow, valueHigh - valueLow));

            chunkMap.compute(
                    measurement.station(),
                    (k, acc) -> (acc == null) ? MutableAccumulator.from(measurement) : acc.add(measurement));
        }

        return chunkMap;
    }

    // Note: using ByteBuffer for easily available hash-function
    public record Measurement(ByteBuffer station, int value) {

        public static Measurement parse(MemorySegment stationData, MemorySegment valueData) {
            var station = stationData.asByteBuffer();
            var value = parseValue(valueData);
            return new Measurement(station, value);
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
            return STR."\{round((double) min / 10)}/\{round((double) sum / 10 / count)}/\{round((double) max / 10)}";
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        // Note - used for debugging
        @Override
        public String toString() {
            return STR."{min: \{min}, max: \{max}, sum: \{sum}, count: \{count}}";
        }
    }

}
