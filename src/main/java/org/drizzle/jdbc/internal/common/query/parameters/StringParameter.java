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

import static org.drizzle.jdbc.internal.common.Utils.sqlEscapeString;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

/**
 * Parameter holder for SQL string values with automatic SQL escaping and UTF-8 encoding.
 * 
 * <p>For strings <= 32KB, the entire value is buffered in memory.
 * For larger strings, streaming encoding is used to minimize memory usage.</p>
 * 
 * <p><strong>Thread Safety:</strong> This class is NOT thread-safe. Each instance
 * should only be accessed by a single thread or externally synchronized.</p>
 * 
 * User: marcuse Date: Feb 18, 2009 Time: 10:17:14 PM
 */
public class StringParameter implements ParameterHolder {
    private final byte[] byteRepresentation;

    private final boolean fullyBuffered;
    private final long bytesLength;

    // Shared encoding state fields
    private CharsetEncoder encoder;
    private CharBuffer charBuffer;
    private ByteBuffer byteBuffer;
    private boolean encodingComplete = false;


    // 32K buffer size: balances memory usage vs. encoding efficiency
    // Strings <= 32K characters are fully buffered for simplicity
    // If streaming (32K+ characters strings), this will be used as the internal
    // buffer size
    private static final int BUFFER_SIZE = 32768;

    public StringParameter(final String parameter) {
        final String param = "'" + sqlEscapeString(parameter) + "'";

        // For small strings, keep old behavior
        if (param.length() <= BUFFER_SIZE) {
            this.byteRepresentation = param.getBytes(StandardCharsets.UTF_8);
            this.bytesLength = this.byteRepresentation.length;
            fullyBuffered = true;
        } else {
            fullyBuffered = false;
            this.byteRepresentation = null;
            encoder = StandardCharsets.UTF_8.newEncoder();
            charBuffer = CharBuffer.wrap(param);
            byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            this.bytesLength = countBytesInternal();
        }
    }

    /**
     * Resets all encoding state fields for a fresh write. Safe to call between
     * streaming writes or after counting bytes.
     */
    private void resetEncoderBuffers() {
        encoder.reset();
        charBuffer.position(0);
        byteBuffer.clear();
        byteBuffer.flip();
        encodingComplete = false;
    }

    /**
     * Byte counting and size calculation use the shared buffers for efficiency.
     * Resets encoder state after counting to prepare for writing.
     */
    private long countBytesInternal() {
        long totalBytes = 0;

        // Use endOfInput=false for all iterations except the last
        while (charBuffer.hasRemaining()) {
            CoderResult result = encoder.encode(charBuffer, byteBuffer, false);
            totalBytes += byteBuffer.position();
            byteBuffer.clear();

            // If encoder returns OVERFLOW, the buffer is too small but encoding continues
            // If encoder returns UNDERFLOW, all input was consumed (expected)
            // If encoder returns ERROR, there's a malformed input
            if (result.isError()) {
                throw new RuntimeException("Character encoding error: " + result.toString());
            }
        }

        // Signal end of input and flush any remaining bytes
        CoderResult result = encoder.encode(charBuffer, byteBuffer, true);  // Final call with endOfInput=true
        if (result.isError()) {
            throw new RuntimeException("Character encoding error at end: " + result.toString());
        }

        result = encoder.flush(byteBuffer);
        if (result.isError()) {
            throw new RuntimeException("Character encoding flush error: " + result.toString());
        }

        totalBytes += byteBuffer.position();

        // Reset for subsequent writes
        resetEncoderBuffers();

        return totalBytes;
    }

    /**
     * Returns total size in bytes.
     */
    public long length() {
        return bytesLength;
    }

    /**
     * Write at most maxWriteSize, return the amount actually written
     *
     * @param os           the stream to write to
     * @param offset       where to start writing
     * @param maxWriteSize max number of bytes to write
     * @return the number of bytes written (either maxWriteSize or the length of the parameter)
     * @throws IOException when everything goes wrong
     */
    public int writeTo(final OutputStream os, int offset, int maxWriteSize) throws IOException {
        if (fullyBuffered) {
            return writeFullyBufferedInput(os, offset, maxWriteSize);
        } else {
            return writeNextBytesFromStreamedInput(os, maxWriteSize);
        }
    }

    /**
     * Efficient streaming implementation that resumes writing across calls. After a
     * full write, call resetEncoderBuffers() to reuse from the beginning.
     * 
     * @param os           the output stream to write to
     * @param offset       the offset in the byte array (ignored for non-buffered
     *                     strings)
     * @param maxWriteSize maximum number of bytes to write
     * @return the number of bytes actually written
     * @throws IOException if an I/O error occurs
     */
    private int writeNextBytesFromStreamedInput(final OutputStream os, int maxWriteSize) throws IOException {
        int totalBytesWritten = 0;
        int bytesToWrite = 0;
        int bufferRemaining = byteBuffer.remaining();

        // If the previously read buffer was not fully sent (split across several
        // packets), first send the remaining bytes from buffer before reading more
        // bytes to send
        if (bufferRemaining > 0) {
            bytesToWrite = Math.min(bufferRemaining, maxWriteSize - totalBytesWritten);
            os.write(byteBuffer.array(), byteBuffer.position(), bytesToWrite);
            byteBuffer.position(byteBuffer.position() + bytesToWrite);

            totalBytesWritten += bytesToWrite;

            if (totalBytesWritten >= maxWriteSize) {
                // Stream is done once there is nothing left in charBuffer or in byteBuffer
                return totalBytesWritten;
            }
        }
        // Everything was sent so far, get more data from input and send it
        // Use endOfInput=false during normal encoding
        while (charBuffer.hasRemaining()) {
            byteBuffer.clear();
            encoder.encode(charBuffer, byteBuffer, false);
            byteBuffer.flip();
            bufferRemaining = byteBuffer.remaining();
            bytesToWrite = Math.min(bufferRemaining, maxWriteSize - totalBytesWritten);
            os.write(byteBuffer.array(), byteBuffer.position(), bytesToWrite);
            byteBuffer.position(byteBuffer.position() + bytesToWrite);
            totalBytesWritten += bytesToWrite;
            if (totalBytesWritten >= maxWriteSize) {
                return totalBytesWritten;
            }
        }

        if (!encodingComplete) {
            // After consuming all input, perform the final encoding step
            byteBuffer.clear();
            encoder.encode(charBuffer, byteBuffer, true);  // Signal end of input
            encoder.flush(byteBuffer);  // Flush any remaining bytes
            byteBuffer.flip();
            encodingComplete = true;

            // Write any remaining bytes from the final flush
            bufferRemaining = byteBuffer.remaining();
            if (bufferRemaining > 0) {
                bytesToWrite = Math.min(bufferRemaining, maxWriteSize - totalBytesWritten);
                os.write(byteBuffer.array(), byteBuffer.position(), bytesToWrite);
                byteBuffer.position(byteBuffer.position() + bytesToWrite);
                totalBytesWritten += bytesToWrite;
            }
        }
        return totalBytesWritten;
    }

    private int writeFullyBufferedInput(final OutputStream os, int offset, int maxWriteSize) throws IOException {
        int bytesToWrite = Math.min(byteRepresentation.length - offset, maxWriteSize);
        os.write(byteRepresentation, offset, bytesToWrite);
        return bytesToWrite;
    }
}
