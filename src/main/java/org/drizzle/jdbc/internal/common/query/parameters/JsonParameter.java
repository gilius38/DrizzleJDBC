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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * User: marcuse Date: Feb 18, 2009 Time: 10:17:14 PM
 */
public class JsonParameter implements ParameterHolder {
    private final byte[] byteRepresentation;

    public JsonParameter(final String parameter) {
        final String tempParam = jsonEscapeString(parameter);
        try {
            this.byteRepresentation = tempParam.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("unsupp encoding: " + e.getMessage(), e);
        }
    }

    public int writeTo(final OutputStream os, int offset, int maxWriteSize) throws IOException {
        int bytesToWrite = Math.min(byteRepresentation.length - offset, maxWriteSize);
        os.write(byteRepresentation, offset, bytesToWrite);
        return bytesToWrite;
    }

    public long length() {
        return byteRepresentation.length;
    }
    /**
     * escapes the given string, new string length is at most twice the length of str
     *
     * @param str the string to escape
     * @return an escaped string
     */
    private String jsonEscapeString(final String str) {
        StringBuilder buffer = new StringBuilder(str.length() * 2);
        boolean neededEscaping = false;
        for (int i = 0; i < str.length(); i++) {
            final char c = str.charAt(i);
            if (((byte) c) == '\\') {
                neededEscaping = true;
                buffer.append('\\');
            }
            buffer.append(c);
        }
        return neededEscaping ? buffer.toString() : str;
    }

}
