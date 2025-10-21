/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Comprehensive tests for StringParameter class covering both buffered and streaming modes.
 */
public class StringParameterTest {

    // ========================================================================
    // BUFFERED MODE TESTS (strings <= 32KB after escaping)
    // ========================================================================

    @Test
    public void testSimpleString() throws IOException {
        System.out.println(">>> Running: testSimpleString");
        StringParameter param = new StringParameter("hello");

        // Expected: 'hello' with quotes
        String expected = "'hello'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testEmptyString() throws IOException {
        System.out.println(">>> Running: testEmptyString");
        StringParameter param = new StringParameter("");

        // Expected: '' (just quotes)
        String expected = "''";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testSingleCharacter() throws IOException {
        System.out.println(">>> Running: testSingleCharacter");
        StringParameter param = new StringParameter("x");

        String expected = "'x'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStringWithSingleQuotes() throws IOException {
        System.out.println(">>> Running: testStringWithSingleQuotes");
        StringParameter param = new StringParameter("it's");

        // Single quotes should be escaped
        String expected = "'it\\'s'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStringWithDoubleQuotes() throws IOException {
        System.out.println(">>> Running: testStringWithDoubleQuotes");
        StringParameter param = new StringParameter("say \"hello\"");

        // Double quotes should be escaped
        String expected = "'say \\\"hello\\\"'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStringWithBackslash() throws IOException {
        System.out.println(">>> Running: testStringWithBackslash");
        StringParameter param = new StringParameter("path\\to\\file");

        // Backslashes should be escaped
        String expected = "'path\\\\to\\\\file'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStringWithNewline() throws IOException {
        System.out.println(">>> Running: testStringWithNewline");
        StringParameter param = new StringParameter("line1\nline2");

        // Newlines should be escaped (backslash added before the actual newline char)
        String expected = "'line1\\\nline2'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStringWithCarriageReturn() throws IOException {
        System.out.println(">>> Running: testStringWithCarriageReturn");
        StringParameter param = new StringParameter("line1\rline2");

        // Carriage returns should be escaped (backslash added before the actual CR char)
        String expected = "'line1\\\rline2'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStringWithNullByte() throws IOException {
        System.out.println(">>> Running: testStringWithNullByte");
        StringParameter param = new StringParameter("before\u0000after");

        // Null bytes should be escaped (backslash added before the null byte)
        String expected = "'before\\\u0000after'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStringWithCtrlZ() throws IOException {
        System.out.println(">>> Running: testStringWithCtrlZ");
        StringParameter param = new StringParameter("before\u001aafter");

        // Ctrl-Z should be escaped (backslash added before the ctrl-z char)
        String expected = "'before\\\u001aafter'";
        assertParameterEquals(expected, param);
    }

    // ========================================================================
    // UNICODE TESTS
    // ========================================================================

    @Test
    public void testUnicodeString() throws IOException {
        System.out.println(">>> Running: testUnicodeString");
        // Japanese characters
        StringParameter param = new StringParameter("日本語");

        String expected = "'日本語'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testEmojiString() throws IOException {
        System.out.println(">>> Running: testEmojiString");
        // Emojis (4-byte UTF-8)
        StringParameter param = new StringParameter("Hello 👋 World 🌍");

        String expected = "'Hello 👋 World 🌍'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testMixedUnicodeAndEscapes() throws IOException {
        System.out.println(">>> Running: testMixedUnicodeAndEscapes");
        StringParameter param = new StringParameter("日本語's 'test'");

        String expected = "'日本語\\'s \\'test\\''";
        assertParameterEquals(expected, param);
    }

    // ========================================================================
    // BUFFERED MODE: PARTIAL WRITES WITH OFFSET
    // ========================================================================

    @Test
    public void testBufferedPartialWriteWithOffset() throws IOException {
        System.out.println(">>> Running: testBufferedPartialWriteWithOffset");
        StringParameter param = new StringParameter("hello");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Write first 3 bytes
        int written = param.writeTo(baos, 0, 3);
        assertEquals(3, written);

        // Write next 2 bytes
        written = param.writeTo(baos, 3, 2);
        assertEquals(2, written);

        // Write remaining bytes
        written = param.writeTo(baos, 5, 100);
        assertEquals(2, written);  // Only 2 bytes left: o'

        String result = baos.toString("UTF-8");
        assertEquals("'hello'", result);
    }

    @Test
    public void testBufferedWriteExactLength() throws IOException {
        System.out.println(">>> Running: testBufferedWriteExactLength");
        StringParameter param = new StringParameter("test");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        long length = param.length();
        int written = param.writeTo(baos, 0, (int)length);

        assertEquals(length, written);
        assertEquals("'test'", baos.toString("UTF-8"));
    }

    // ========================================================================
    // BOUNDARY TESTS: 32KB THRESHOLD
    // ========================================================================

    @Test
    public void testStringJustUnderThreshold() throws IOException {
        System.out.println(">>> Running: testStringJustUnderThreshold");
        // Create string that's just under 32KB after escaping and quotes
        // 32768 chars total threshold, minus 2 for quotes = 32766 chars, minus 1 = 32765 chars
        String input = createString('x', 32765);
        StringParameter param = new StringParameter(input);

        // Should use buffered mode (32765 + 2 quotes = 32767 < 32768)
        String expected = "'" + input + "'";
        assertParameterEquals(expected, param);
        assertEquals(32767, param.length());
    }

    @Test
    public void testStringAtThreshold() throws IOException {
        System.out.println(">>> Running: testStringAtThreshold");
        // Exactly 32KB after adding quotes (32766 + 2 = 32768)
        String input = createString('x', 32766);
        StringParameter param = new StringParameter(input);

        assertEquals(32768, param.length());
        // Should use buffered mode (param.length() <= 32768)
        assertParameterEquals("'" + input + "'", param);
    }

    @Test
    public void testStringJustOverThreshold() throws IOException {
        System.out.println(">>> Running: testStringJustOverThreshold");
        // Just over 32KB - should trigger streaming mode
        String input = createString('x', 32767);
        StringParameter param = new StringParameter(input);

        // Should use streaming mode
        String expected = "'" + input + "'";
        assertParameterEquals(expected, param);
    }

    // ========================================================================
    // STREAMING MODE TESTS (strings > 32KB)
    // ========================================================================

    @Test
    public void testLargeString() throws IOException {
        System.out.println(">>> Running: testLargeString");
        // 100KB string
        String input = createString('A', 100000);
        StringParameter param = new StringParameter(input);

        String expected = "'" + input + "'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testLargeStringWithUnicode() throws IOException {
        System.out.println(">>> Running: testLargeStringWithUnicode");
        // Large string with multi-byte characters
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            sb.append("日本語");  // 3 chars, 9 bytes each
        }
        String input = sb.toString();

        StringParameter param = new StringParameter(input);
        String expected = "'" + input + "'";
        assertParameterEquals(expected, param);
    }

    @Test
    public void testStreamingPartialWrites() throws IOException {
        System.out.println(">>> Running: testStreamingPartialWrites");
        // Large string written in chunks
        String input = createString('B', 50000);
        StringParameter param = new StringParameter(input);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalLength = param.length();
        long totalWritten = 0;

        // Write in 1KB chunks
        // NOTE: For streaming mode, always use offset 0 (internal position is maintained)
        while (totalWritten < totalLength) {
            int written = param.writeTo(baos, 0, 1024);
            assertTrue("Should write at least 1 byte", written > 0);
            totalWritten += written;
        }

        assertEquals(totalLength, totalWritten);
        String result = baos.toString("UTF-8");
        assertEquals("'" + input + "'", result);
    }

    @Test
    public void testStreamingVerySmallChunks() throws IOException {
        System.out.println(">>> Running: testStreamingVerySmallChunks");
        // Test streaming with very small write sizes (stress test)
        String input = createString('C', 40000);
        StringParameter param = new StringParameter(input);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalLength = param.length();
        long totalWritten = 0;

        // Write in tiny 10-byte chunks
        // NOTE: For streaming mode, always use offset 0 (internal position is maintained)
        while (totalWritten < totalLength) {
            int written = param.writeTo(baos, 0, 10);
            assertTrue("Should write between 1-10 bytes", written > 0 && written <= 10);
            totalWritten += written;
        }

        assertEquals(totalLength, totalWritten);
        String result = baos.toString("UTF-8");
        assertEquals("'" + input + "'", result);
    }

    @Test
    public void testStreamingWithUnicodeAtBoundaries() throws IOException {
        System.out.println(">>> Running: testStreamingWithUnicodeAtBoundaries");
        // Create string where multi-byte UTF-8 chars might split across buffer boundaries
        StringBuilder sb = new StringBuilder();

        // Fill to near 32KB threshold, then add unicode that crosses boundary
        sb.append(createString('x', 32700));
        sb.append("日本語文字列");  // Japanese text
        sb.append(createString('y', 10000));

        String input = sb.toString();
        StringParameter param = new StringParameter(input);

        // Write in chunks that might split multi-byte sequences
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalWritten = 0;
        long totalLength = param.length();

        while (totalWritten < totalLength) {
            // NOTE: For streaming mode, always use offset 0 (internal position is maintained)
            int written = param.writeTo(baos, 0, 100);
            totalWritten += written;
        }

        String result = baos.toString("UTF-8");
        assertEquals("'" + input + "'", result);
    }

    @Test
    public void testStreamingWithEmojisAtBoundaries() throws IOException {
        System.out.println(">>> Running: testStreamingWithEmojisAtBoundaries");
        // Emojis are 4-byte UTF-8 sequences
        StringBuilder sb = new StringBuilder();
        sb.append(createString('z', 32700));

        // Add emojis that will cross buffer boundaries
        for (int i = 0; i < 1000; i++) {
            sb.append("😀🎉🌟");
        }

        String input = sb.toString();
        StringParameter param = new StringParameter(input);

        String expected = "'" + input + "'";
        assertParameterEquals(expected, param);
    }

    // ========================================================================
    // EDGE CASES
    // ========================================================================

    @Test
    public void testLargeStringAllSpecialChars() throws IOException {
        System.out.println(">>> Running: testLargeStringAllSpecialChars");
        // String that will double in size due to escaping
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20000; i++) {
            sb.append("'\\\"");  // Characters that need escaping
        }
        String input = sb.toString();

        StringParameter param = new StringParameter(input);

        // Verify escaping worked correctly
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalWritten = 0;
        while (totalWritten < param.length()) {
            // NOTE: For streaming mode, always use offset 0 (internal position is maintained)
            int written = param.writeTo(baos, 0, 1024);
            totalWritten += written;
        }

        String result = baos.toString("UTF-8");
        assertTrue("Should start with '", result.startsWith("'"));
        assertTrue("Should end with '", result.endsWith("'"));
        assertTrue("Should contain escaped quotes", result.contains("\\'"));
        assertTrue("Should contain escaped backslashes", result.contains("\\\\"));
    }

    @Test
    public void testLengthCalculation() throws IOException {
        System.out.println(">>> Running: testLengthCalculation");
        // Test that length() returns accurate byte count
        String input = "Hello 日本語 👋";
        StringParameter param = new StringParameter(input);

        // Write entire parameter
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int totalWritten = 0;
        while (totalWritten < param.length()) {
            // NOTE: For streaming mode, always use offset 0 (internal position is maintained)
            int written = param.writeTo(baos, 0, 1000);
            totalWritten += written;
        }

        // Verify length matches actual bytes written
        assertEquals(param.length(), baos.size());
    }

    @Test
    public void testLengthCalculationLargeString() throws IOException {
        System.out.println(">>> Running: testLengthCalculationLargeString");
        // Test length calculation for streaming mode
        String input = createString('D', 60000);
        StringParameter param = new StringParameter(input);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalWritten = 0;
        while (totalWritten < param.length()) {
            // NOTE: For streaming mode, always use offset 0 (internal position is maintained)
            int written = param.writeTo(baos, 0, 8192);
            totalWritten += written;
        }

        assertEquals(param.length(), baos.size());
    }

    @Test
    public void testWriteMoreThanAvailable() throws IOException {
        System.out.println(">>> Running: testWriteMoreThanAvailable");
        StringParameter param = new StringParameter("short");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Try to write more than available
        int written = param.writeTo(baos, 0, 1000000);

        // Should only write actual length
        assertEquals(param.length(), written);
        assertEquals("'short'", baos.toString("UTF-8"));
    }

    @Test
    public void testZeroMaxWriteSize() throws IOException {
        System.out.println(">>> Running: testZeroMaxWriteSize");
        StringParameter param = new StringParameter("test");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        int written = param.writeTo(baos, 0, 0);

        assertEquals(0, written);
        assertEquals(0, baos.size());
    }

    // ========================================================================
    // PERFORMANCE TESTS (Optional, can be @Ignore for regular test runs)
    // ========================================================================

    @Test
    public void testPerformanceBufferedMode() throws IOException {
        System.out.println(">>> Running: testPerformanceBufferedMode");
        // Measure time for buffered mode
        String input = createString('E', 10000);

        long startTime = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            StringParameter param = new StringParameter(input);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            param.writeTo(baos, 0, Integer.MAX_VALUE);
        }
        long elapsed = System.nanoTime() - startTime;

        System.out.println("Buffered mode (10K string, 1000 iterations): " +
                          (elapsed / 1_000_000) + "ms");
    }

    @Test
    public void testPerformanceStreamingMode() throws IOException {
        System.out.println(">>> Running: testPerformanceStreamingMode");
        // Measure time for streaming mode
        String input = createString('F', 50000);

        long startTime = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            StringParameter param = new StringParameter(input);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            long written = 0;
            while (written < param.length()) {
                // NOTE: For streaming mode, always use offset 0 (internal position is maintained)
                written += param.writeTo(baos, 0, 8192);
            }
        }
        long elapsed = System.nanoTime() - startTime;

        System.out.println("Streaming mode (50K string, 100 iterations): " +
                          (elapsed / 1_000_000) + "ms");
    }

    // ========================================================================
    // BOUNDARY COMPARISON TESTS: STREAMING VS BUFFERED
    // ========================================================================

    @Test
    public void testBoundaryComparisonExact32KB() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonExact32KB");
        // Exactly at 32768 boundary (32766 chars + 2 quotes = 32768 bytes)
        String input = createString('X', 32766);
        String expected = "'" + input + "'";

        // This should use BUFFERED mode (length == 32768)
        StringParameter param1 = new StringParameter(input);
        assertEquals(32768, param1.length());

        // Just verify it writes correctly in one go
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int written = param1.writeTo(baos, 0, Integer.MAX_VALUE);
        assertEquals(32768, written);
        assertEquals(expected, baos.toString("UTF-8"));
    }

    @Test
    public void testBoundaryComparisonOneBelowThreshold() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonOneBelowThreshold");
        // One byte below threshold (32765 chars + 2 quotes = 32767 bytes)
        String input = createString('Y', 32765);
        StringParameter param = new StringParameter(input);

        // This should use BUFFERED mode (length < 32768)
        assertEquals(32767, param.length());

        String expected = "'" + input + "'";
        assertParameterEquals(expected, param);

        // Test partial writes with offset (buffered mode specific)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int written1 = param.writeTo(baos, 0, 10000);
        assertEquals(10000, written1);

        int written2 = param.writeTo(baos, 10000, 10000);
        assertEquals(10000, written2);

        int written3 = param.writeTo(baos, 20000, 20000);
        assertEquals(12767, written3);  // Only 12767 bytes remaining

        assertEquals(expected, baos.toString("UTF-8"));
    }

    @Test
    public void testBoundaryComparisonOneAboveThreshold() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonOneAboveThreshold");
        // One byte above threshold (32767 chars + 2 quotes = 32769 bytes)
        String input = createString('Z', 32767);
        StringParameter param = new StringParameter(input);

        // This should use STREAMING mode (length > 32768)
        assertEquals(32769, param.length());

        String expected = "'" + input + "'";
        assertParameterEquals(expected, param);

        // Test streaming partial writes (create fresh instances for each test)
        verifyWriteInChunks(new StringParameter(input), expected, 8192);
        verifyWriteInChunks(new StringParameter(input), expected, 1000);
        verifyWriteInChunks(new StringParameter(input), expected, 1);  // Extreme case
    }

    @Test
    public void testBoundaryComparisonWithMultiByteChars() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonWithMultiByteChars");
        // Create strings with 3-byte UTF-8 characters near the boundary

        // Just below: 10921 chars * 3 bytes = 32763 bytes + 2 quotes = 32765 (BUFFERED)
        StringBuilder sbBelow = new StringBuilder();
        for (int i = 0; i < 10921; i++) {
            sbBelow.append("中");  // 3-byte UTF-8 character
        }
        String inputBelow = sbBelow.toString();
        StringParameter paramBelow = new StringParameter(inputBelow);
        assertTrue("Should use buffered mode", paramBelow.length() <= 32768);
        assertEquals(32765, paramBelow.length());

        // Just above: 10923 chars * 3 bytes = 32769 bytes + 2 quotes = 32771 (STREAMING)
        StringBuilder sbAbove = new StringBuilder();
        for (int i = 0; i < 10923; i++) {
            sbAbove.append("中");
        }
        String inputAbove = sbAbove.toString();
        StringParameter paramAbove = new StringParameter(inputAbove);
        assertTrue("Should use streaming mode", paramAbove.length() > 32768);
        assertEquals(32771, paramAbove.length());

        // Verify both work correctly
        assertParameterEquals("'" + inputBelow + "'", paramBelow);
        assertParameterEquals("'" + inputAbove + "'", paramAbove);
    }

    @Test
    public void testBoundaryComparisonWithEmojis() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonWithEmojis");
        // Create strings with 4-byte UTF-8 characters (emojis) near the boundary

        // Just below: 8191 emojis * 4 bytes = 32764 bytes + 2 quotes = 32766 (BUFFERED)
        StringBuilder sbBelow = new StringBuilder();
        for (int i = 0; i < 8191; i++) {
            sbBelow.append("🎉");  // 4-byte UTF-8 character
        }
        String inputBelow = sbBelow.toString();
        StringParameter paramBelow = new StringParameter(inputBelow);
        assertTrue("Should use buffered mode", paramBelow.length() <= 32768);
        assertEquals(32766, paramBelow.length());

        // Just above: 8192 emojis * 4 bytes = 32768 bytes + 2 quotes = 32770 (STREAMING)
        StringBuilder sbAbove = new StringBuilder();
        for (int i = 0; i < 8192; i++) {
            sbAbove.append("🎉");
        }
        String inputAbove = sbAbove.toString();
        StringParameter paramAbove = new StringParameter(inputAbove);
        assertTrue("Should use streaming mode", paramAbove.length() > 32768);
        assertEquals(32770, paramAbove.length());

        // Verify both work correctly
        assertParameterEquals("'" + inputBelow + "'", paramBelow);
        assertParameterEquals("'" + inputAbove + "'", paramAbove);
    }

    @Test
    public void testBoundaryComparisonPerformance() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonPerformance");

        int iterations = 100;

        // Just below threshold (buffered mode)
        String inputBuffered = createString('B', 32766);
        long bufferedTime = measureWritePerformance(inputBuffered, iterations);

        // Just above threshold (streaming mode)
        String inputStreaming = createString('S', 32767);
        long streamingTime = measureWritePerformance(inputStreaming, iterations);

        System.out.println("Buffered mode (32766 chars, " + iterations + " iterations): " +
                          (bufferedTime / 1_000_000) + "ms");
        System.out.println("Streaming mode (32767 chars, " + iterations + " iterations): " +
                          (streamingTime / 1_000_000) + "ms");

        // Streaming should be reasonably close to buffered for similar sizes
        // This is not a strict assertion, just informational
        double ratio = (double) streamingTime / bufferedTime;
        System.out.println("Streaming/Buffered ratio: " + String.format("%.2f", ratio));

        // Sanity check: streaming shouldn't be more than 10x slower for similar sizes
        assertTrue("Streaming should be reasonably efficient", ratio < 10.0);
    }

    @Test
    public void testBoundaryComparisonMemoryFootprint() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonMemoryFootprint");

        // Buffered mode: entire string in memory
        String inputBuffered = createString('M', 32766);
        StringParameter paramBuffered = new StringParameter(inputBuffered);

        // Streaming mode: only buffer in memory (32KB)
        String inputStreaming = createString('M', 100000);
        StringParameter paramStreaming = new StringParameter(inputStreaming);

        // Write both and verify correctness
        assertParameterEquals("'" + inputBuffered + "'", paramBuffered);
        assertParameterEquals("'" + inputStreaming + "'", paramStreaming);

        // Note: Actual memory measurement would require JMH or similar
        // This test just verifies both modes work correctly
        System.out.println("Buffered mode length: " + paramBuffered.length() + " bytes");
        System.out.println("Streaming mode length: " + paramStreaming.length() + " bytes");
    }

    @Test
    public void testBoundaryComparisonIdenticalResults() throws IOException {
        System.out.println(">>> Running: testBoundaryComparisonIdenticalResults");

        // Create identical content, one buffered, one streaming
        String content = createString('I', 20000);

        // Buffered version (prepend to make it small)
        String inputBuffered = content.substring(0, 10000);
        StringParameter paramBuffered = new StringParameter(inputBuffered);

        // Streaming version (use full content to exceed threshold)
        String inputStreaming = content + content;  // 40000 chars
        StringParameter paramStreaming = new StringParameter(inputStreaming);

        // Both should produce identical results for their respective content
        ByteArrayOutputStream baosBuffered = new ByteArrayOutputStream();
        ByteArrayOutputStream baosStreaming = new ByteArrayOutputStream();

        // Write buffered
        int totalBuffered = 0;
        while (totalBuffered < paramBuffered.length()) {
            int written = paramBuffered.writeTo(baosBuffered, totalBuffered, 1000);
            totalBuffered += written;
        }

        // Write streaming
        long totalStreaming = 0;
        while (totalStreaming < paramStreaming.length()) {
            int written = paramStreaming.writeTo(baosStreaming, 0, 1000);
            if (written == 0) break;
            totalStreaming += written;
        }

        assertEquals("'" + inputBuffered + "'", baosBuffered.toString("UTF-8"));
        assertEquals("'" + inputStreaming + "'", baosStreaming.toString("UTF-8"));
    }

    @Test
    public void testBoundaryWithSQLEscaping() throws IOException {
        System.out.println(">>> Running: testBoundaryWithSQLEscaping");

        // Create string with characters that need escaping, near boundary
        StringBuilder sb = new StringBuilder();
        // Create a string that will exceed 32KB after SQL escaping
        for (int i = 0; i < 11000; i++) {
            sb.append("'test\\");  // Each quote and backslash will be escaped
        }

        String input = sb.toString();
        StringParameter param = new StringParameter(input);

        // After escaping and quotes, should exceed 32KB and use streaming
        assertTrue("Should use streaming mode after escaping", param.length() > 32768);

        // Verify correctness by writing completely
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalWritten = 0;
        int maxIterations = 1000;  // Safety limit
        int iterations = 0;
        while (totalWritten < param.length() && iterations < maxIterations) {
            int written = param.writeTo(baos, 0, 1000);
            if (written == 0) break;
            totalWritten += written;
            iterations++;
        }

        assertEquals("Should write all bytes", param.length(), totalWritten);
        String result = baos.toString("UTF-8");
        assertTrue("Should start with quote", result.startsWith("'"));
        assertTrue("Should end with quote", result.endsWith("'"));
        assertTrue("Should contain escaped quotes", result.contains("\\'"));
        assertTrue("Should contain escaped backslashes", result.contains("\\\\"));
    }

    // ========================================================================
    // CHUNK SIZE COMPARISON TESTS
    // ========================================================================

    @Test
    public void testVariousChunkSizes() throws IOException {
        System.out.println(">>> Running: testVariousChunkSizes");

        // Test with a moderately large string (100KB)
        String input = createString('T', 100000);
        String expected = "'" + input + "'";

        // Test various chunk sizes and verify all produce the same result
        int[] chunkSizes = {1, 10, 100, 512, 1024, 4096, 8192, 16384, 32768, 65536};

        for (int chunkSize : chunkSizes) {
            StringParameter param = new StringParameter(input);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            long totalWritten = 0;

            while (totalWritten < param.length()) {
                int written = param.writeTo(baos, 0, chunkSize);
                if (written == 0) break;
                totalWritten += written;
            }

            assertEquals("Chunk size " + chunkSize + " should write all bytes",
                        param.length(), totalWritten);
            assertEquals("Chunk size " + chunkSize + " should produce correct output",
                        expected, baos.toString("UTF-8"));
        }
    }

    @Test
    public void testChunkSizePerformanceComparison() throws IOException {
        System.out.println(">>> Running: testChunkSizePerformanceComparison");

        // Test with a large string (200KB)
        String input = createString('P', 200000);
        int iterations = 50;

        // Test various chunk sizes
        int[] chunkSizes = {512, 1024, 4096, 8192, 16384, 32768};

        System.out.println("\nChunk Size Performance (200KB string, " + iterations + " iterations):");
        System.out.println("Chunk Size | Time (ms) | Writes | Overhead");
        System.out.println("-----------|-----------|--------|----------");

        long baselineTime = 0;
        for (int chunkSize : chunkSizes) {
            long startTime = System.nanoTime();
            int totalWrites = 0;

            for (int i = 0; i < iterations; i++) {
                StringParameter param = new StringParameter(input);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                long totalWritten = 0;

                while (totalWritten < param.length()) {
                    int written = param.writeTo(baos, 0, chunkSize);
                    if (written == 0) break;
                    totalWritten += written;
                    totalWrites++;
                }
            }

            long elapsed = System.nanoTime() - startTime;
            long elapsedMs = elapsed / 1_000_000;
            int avgWrites = totalWrites / iterations;

            if (baselineTime == 0) {
                baselineTime = elapsed;
                System.out.printf("%10d | %9d | %6d | baseline\n",
                                chunkSize, elapsedMs, avgWrites);
            } else {
                double overhead = ((double) elapsed / baselineTime - 1.0) * 100;
                System.out.printf("%10d | %9d | %6d | %+.1f%%\n",
                                chunkSize, elapsedMs, avgWrites, overhead);
            }
        }
    }

    @Test
    public void testChunkSizeWithMultiByteCharacters() throws IOException {
        System.out.println(">>> Running: testChunkSizeWithMultiByteCharacters");

        // Create string with mix of 1, 2, 3, and 4-byte UTF-8 characters
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append("Hello");        // 1-byte chars
            sb.append("Café");         // 2-byte char (é)
            sb.append("世界");         // 3-byte chars
            sb.append("🌍🎉");         // 4-byte chars
        }
        String input = sb.toString();
        String expected = "'" + input + "'";

        // Test with various chunk sizes, including very small ones that might
        // split multi-byte characters
        int[] chunkSizes = {1, 3, 7, 13, 100, 1000, 10000};

        for (int chunkSize : chunkSizes) {
            StringParameter param = new StringParameter(input);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            long totalWritten = 0;

            while (totalWritten < param.length()) {
                int written = param.writeTo(baos, 0, chunkSize);
                if (written == 0) break;
                totalWritten += written;
            }

            assertEquals("Chunk size " + chunkSize + " should write all bytes",
                        param.length(), totalWritten);

            String result = baos.toString("UTF-8");
            assertEquals("Chunk size " + chunkSize + " should produce correct UTF-8",
                        expected, result);

            // Verify specific multi-byte characters are intact
            assertTrue("Should contain emoji", result.contains("🌍"));
            assertTrue("Should contain Chinese", result.contains("世界"));
            assertTrue("Should contain accented char", result.contains("Café"));
        }
    }

    @Test
    public void testOptimalChunkSizeRecommendation() throws IOException {
        System.out.println(">>> Running: testOptimalChunkSizeRecommendation");

        // Test with various string sizes to find optimal chunk size patterns
        int[] stringSizes = {1000, 10000, 50000, 100000, 500000};
        int[] chunkSizes = {1024, 4096, 8192, 16384, 32768};

        System.out.println("\nOptimal Chunk Size Analysis:");
        System.out.println("String Size | Recommended Chunk | Reason");
        System.out.println("------------|-------------------|--------");

        for (int stringSize : stringSizes) {
            String input = createString('O', stringSize);
            long bestTime = Long.MAX_VALUE;
            int bestChunkSize = 0;
            int iterations = 20;

            for (int chunkSize : chunkSizes) {
                long startTime = System.nanoTime();

                for (int i = 0; i < iterations; i++) {
                    StringParameter param = new StringParameter(input);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    long totalWritten = 0;

                    while (totalWritten < param.length()) {
                        int written = param.writeTo(baos, 0, chunkSize);
                        if (written == 0) break;
                        totalWritten += written;
                    }
                }

                long elapsed = System.nanoTime() - startTime;
                if (elapsed < bestTime) {
                    bestTime = elapsed;
                    bestChunkSize = chunkSize;
                }
            }

            String reason;
            if (stringSize <= 10000) {
                reason = "Small string, overhead matters";
            } else if (stringSize <= 100000) {
                reason = "Medium string, balance needed";
            } else {
                reason = "Large string, throughput priority";
            }

            System.out.printf("%11d | %17d | %s\n",
                            stringSize, bestChunkSize, reason);
        }
    }

    @Test
    public void testChunkSizeEdgeCases() throws IOException {
        System.out.println(">>> Running: testChunkSizeEdgeCases");

        String input = createString('E', 50000);
        String expected = "'" + input + "'";

        // Test edge case chunk sizes
        int[] edgeCases = {
            1,           // Minimum
            2,           // Very small
            7,           // Prime number (unusual)
            32767,       // Just below internal buffer
            32768,       // Exactly internal buffer
            32769,       // Just above internal buffer
            65536,       // 2x internal buffer
            Integer.MAX_VALUE  // Maximum
        };

        for (int chunkSize : edgeCases) {
            StringParameter param = new StringParameter(input);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            long totalWritten = 0;

            while (totalWritten < param.length()) {
                int written = param.writeTo(baos, 0, chunkSize);
                if (written == 0) break;
                totalWritten += written;
            }

            assertEquals("Edge case chunk size " + chunkSize + " should work",
                        param.length(), totalWritten);
            assertEquals("Edge case chunk size " + chunkSize + " should produce correct output",
                        expected, baos.toString("UTF-8"));
        }
    }

    @Test
    public void testChunkSizeMemoryEfficiency() throws IOException {
        System.out.println(">>> Running: testChunkSizeMemoryEfficiency");

        // Very large string (1MB)
        String input = createString('M', 1000000);

        // Small chunk size should work without loading entire string into memory
        // (except for the ByteArrayOutputStream accumulating output)
        int smallChunkSize = 512;

        StringParameter param = new StringParameter(input);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalWritten = 0;
        int writeCount = 0;

        while (totalWritten < param.length()) {
            int written = param.writeTo(baos, 0, smallChunkSize);
            if (written == 0) break;
            totalWritten += written;
            writeCount++;
        }

        assertEquals("Should write all bytes", param.length(), totalWritten);
        System.out.println("Wrote " + totalWritten + " bytes in " + writeCount +
                          " writes (" + smallChunkSize + " bytes per write)");

        // Verify correctness
        assertTrue("Output should start with quote", baos.toString("UTF-8").startsWith("'"));
        assertTrue("Output should end with quote", baos.toString("UTF-8").endsWith("'"));
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    /**
     * Creates a string of specified length with repeated character
     */
    private String createString(char c, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Asserts that a parameter, when fully written, produces the expected string
     */
    private void assertParameterEquals(String expected, StringParameter param) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Write entire parameter
        // Strategy: Write in a single call with large maxWriteSize
        // This works for both buffered and streaming modes
        int written = param.writeTo(baos, 0, Integer.MAX_VALUE);

        assertEquals("Should write all bytes in one call", param.length(), written);

        String result = baos.toString("UTF-8");
        assertEquals(expected, result);
        assertEquals("Length should match bytes written", param.length(), baos.size());
    }

    /**
     * Verifies that a parameter can be written in chunks and produces the expected result.
     * This helper method is used to test streaming writes.
     */
    private void verifyWriteInChunks(StringParameter param, String expected, int chunkSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long totalWritten = 0;

        while (totalWritten < param.length()) {
            int written = param.writeTo(baos, 0, chunkSize);
            if (written == 0) break;
            totalWritten += written;
        }

        assertEquals("Should write all bytes", param.length(), totalWritten);
        assertEquals("Should produce expected result", expected, baos.toString("UTF-8"));
    }

    /**
     * Measures the performance of writing a parameter multiple times.
     * Returns the total time in nanoseconds.
     */
    private long measureWritePerformance(String input, int iterations) throws IOException {
        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            StringParameter param = new StringParameter(input);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            long totalWritten = 0;
            while (totalWritten < param.length()) {
                int written = param.writeTo(baos, 0, 8192);
                if (written == 0) break;
                totalWritten += written;
            }
        }

        return System.nanoTime() - startTime;
    }
}
