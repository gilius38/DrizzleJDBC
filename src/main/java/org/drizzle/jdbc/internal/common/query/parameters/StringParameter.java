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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

/**
 * User: marcuse Date: Feb 18, 2009 Time: 10:17:14 PM
 */
public class StringParameter implements ParameterHolder {
    private final byte[] byteRepresentation;

    private final boolean fullyBuffered;
    private final int bytesLength;

    // Shared encoding state fields
    private CharsetEncoder encoder;
    private CharBuffer charBuffer;
    private ByteBuffer byteBuffer;
    private boolean streamDone = false;

    // buffer size
    private static final int BUFFER_SIZE = 32768;

    public StringParameter(final String parameter) {
        final String param = "'" + sqlEscapeString(parameter) + "'";

        // For small strings, keep old behavior
        if (param.length() <= BUFFER_SIZE) {
            try {
                this.byteRepresentation = param.getBytes("UTF-8");
                this.bytesLength = this.byteRepresentation.length;
                fullyBuffered = true;
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
            }
        } else {
            fullyBuffered = false;
            this.byteRepresentation = null;
            encoder = StandardCharsets.UTF_8.newEncoder();
            charBuffer = CharBuffer.wrap(param);
            byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            try {
                this.bytesLength = countBytesInternal();
            } catch (CharacterCodingException e) {
                throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
            }
            resetEncoderBuffers();
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
        streamDone = false;
    }

    /**
     * Byte counting and size calculation use the shared buffers for efficiency.
     */
    private int countBytesInternal() throws CharacterCodingException {
        int totalBytes = 0;
        while (charBuffer.hasRemaining()) {
            encoder.encode(charBuffer, byteBuffer, true);
            totalBytes += byteBuffer.position();
            byteBuffer.clear();
        }
        encoder.flush(byteBuffer);
        totalBytes += byteBuffer.position();
        return totalBytes;
    }

    /**
     * Returns total size in bytes.
     */
    public long length() {
        return bytesLength;
    }

    /**
     * Efficient streaming implementation that resumes writing across calls. After a
     * full write, call resetEncoderBuffers() to reuse from the beginning.
     */
    public int writeTo(final OutputStream os, int offset, int maxWriteSize) throws IOException {
        if (fullyBuffered) {
            int bytesToWrite = Math.min(byteRepresentation.length - offset, maxWriteSize);
            os.write(byteRepresentation, offset, bytesToWrite);
            return bytesToWrite;
        } else {
            /**
             * Efficient streaming implementation that resumes writing across calls. After a
             * full write, call resetEncoderBuffers() to reuse from the beginning. Note that
             * here, offset is useless, since the offset is kept by the byteBuffer position.
             * The length however is used to compute how many bytes should be sent to the
             * protocol.
             */
            if (streamDone)
                return 0;
            int totalBytesWritten = 0;
            int bytesToWrite = 0;
            int bufferRemaining = byteBuffer.remaining();

            // If the previously read buffer was not fully sent (split across several
            // packets), first send the remaining bytes from buffer before reading more
            // bytes to send
            if (bufferRemaining > 0) {
                bytesToWrite = Math.min(bufferRemaining, maxWriteSize - totalBytesWritten);
                os.write(byteBuffer.array(), byteBuffer.position(), bytesToWrite);
                totalBytesWritten += bytesToWrite;

                if (totalBytesWritten >= maxWriteSize) {
                    // Stream is done once there is nothing left in charBuffer or in byteBuffer
                    streamDone = !charBuffer.hasRemaining() && !byteBuffer.hasRemaining();
                    return totalBytesWritten;
                }
            }
            // Everything was sent so far, get more data from input and send it
            while (!streamDone && charBuffer.hasRemaining()) {
                byteBuffer.clear();
                encoder.encode(charBuffer, byteBuffer, true);
                byteBuffer.flip();
                bufferRemaining = byteBuffer.remaining();
                bytesToWrite = Math.min(bufferRemaining, maxWriteSize - totalBytesWritten);
                os.write(byteBuffer.array(), byteBuffer.position(), bytesToWrite);
                byteBuffer.position(byteBuffer.position() + bytesToWrite);
                totalBytesWritten += bytesToWrite;
                if (totalBytesWritten >= maxWriteSize) {
                    break;
                }
            }
            // Stream is done once there is nothing left in charBuffer or in byteBuffer
            streamDone = !charBuffer.hasRemaining() && !byteBuffer.hasRemaining();
            return totalBytesWritten;
        }
    }
}
