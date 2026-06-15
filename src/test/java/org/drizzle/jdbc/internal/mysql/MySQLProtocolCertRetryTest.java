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

package org.drizzle.jdbc.internal.mysql;

import org.drizzle.jdbc.internal.SQLExceptionMapper;
import org.drizzle.jdbc.internal.common.QueryException;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CT-2779: unit tests for the two pure decisions behind the late-TLS-1.3
 * client-cert retry in {@link MySQLProtocol}:
 * <ul>
 *   <li>{@link MySQLProtocol#isLateCertRejection} - is a post-handshake auth
 *       failure a connection-level break (which another keystore alias might
 *       fix) versus a genuine auth / protocol error (which it cannot)?</li>
 *   <li>{@link MySQLProtocol#hasAnotherAliasToTry} - is a further alias attempt
 *       even possible, so the original error is surfaced once aliases run out?</li>
 * </ul>
 * Together these are the rethrow-vs-retry boundary the fix turns on. The full
 * connect path (the loop actually advancing to the next alias, the non-SSL
 * branch, and finishConnect() running once outside the retry scope) needs a
 * live/mock MySQL server and is exercised by the QA harness.
 */
public class MySQLProtocolCertRetryTest {

    private static final String CONN =
            SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState();

    // ---- isLateCertRejection: connection-level state => retry ---------------

    /** A broken pipe wrapped as a CONNECTION_EXCEPTION (what authenticate() does). */
    @Test
    public void brokenPipeIsRetried() {
        QueryException qe = new QueryException("Could not connect: Broken pipe",
                -1, CONN, new SocketException("Broken pipe (Write failed)"));
        assertTrue(MySQLProtocol.isLateCertRejection(qe));
    }

    /** A late TLS alert, also funnelled to CONNECTION_EXCEPTION. */
    @Test
    public void lateTlsAlertIsRetried() {
        QueryException qe = new QueryException("Could not connect", -1, CONN,
                new SSLException("Received fatal alert: decrypt_error"));
        assertTrue(MySQLProtocol.isLateCertRejection(qe));
    }

    /** Keyed on the SQL state, not the cause: a cause-less 08 still retries. */
    @Test
    public void connectionStateWithoutCauseIsRetried() {
        QueryException qe = new QueryException("connection reset", (short) -1, CONN);
        assertTrue(MySQLProtocol.isLateCertRejection(qe));
    }

    // ---- isLateCertRejection: anything else => propagate --------------------

    /** A server ERROR packet (access denied) carries the default state. */
    @Test
    public void serverErrorPacketIsNotRetried() {
        QueryException qe = new QueryException(
                "Could not connect: Access denied for user 'foo'@'host'");
        assertFalse(MySQLProtocol.isLateCertRejection(qe));
    }

    /** An auth-plugin protocol error (sha256 AuthMoreData) is not connection-level. */
    @Test
    public void pluginProtocolErrorIsNotRetried() {
        QueryException qe = new QueryException(
                "Don't know how to read auth more data in sha256 auth plugin!");
        assertFalse(MySQLProtocol.isLateCertRejection(qe));
    }

    /** A non-08 SQL state (e.g. syntax error) is not a cert rejection. */
    @Test
    public void otherSqlStateIsNotRetried() {
        QueryException qe = new QueryException("syntax error", (short) 1064, "42000");
        assertFalse(MySQLProtocol.isLateCertRejection(qe));
    }

    // ---- isRetryableCertFailure: covers RuntimeException late rejections -----

    /** A CONNECTION_EXCEPTION QueryException (transport break) => retry. */
    @Test
    public void connectionExceptionIsRetryable() {
        QueryException qe = new QueryException("broken pipe", -1, CONN,
                new SocketException("Broken pipe"));
        assertTrue(MySQLProtocol.isRetryableCertFailure(qe));
    }

    /** A real auth error (non-08 QueryException, e.g. access denied) => propagate. */
    @Test
    public void authErrorIsNotRetryable() {
        QueryException qe = new QueryException("Access denied for user 'foo'");
        assertFalse(MySQLProtocol.isRetryableCertFailure(qe));
    }

    /** A torn-connection plugin/parse failure (RuntimeException) => retry. */
    @Test
    public void runtimeExceptionIsRetryable() {
        assertTrue(MySQLProtocol.isRetryableCertFailure(
                new RuntimeException("Bad public key format")));
        assertTrue(MySQLProtocol.isRetryableCertFailure(
                new ArrayIndexOutOfBoundsException("truncated packet")));
    }

    /** An Error is not a retryable cert failure. */
    @Test
    public void errorIsNotRetryable() {
        assertFalse(MySQLProtocol.isRetryableCertFailure(new StackOverflowError()));
    }

    // ---- hasAnotherAliasToTry: rethrow-vs-retry exhaustion boundary ---------

    /** Keystore configured but not yet loaded (first, default-alias attempt). */
    @Test
    public void keystoreConfiguredBeforeLoadCanRetry() {
        assertTrue(MySQLProtocol.hasAnotherAliasToTry(null, true));
    }

    /** No keystore at all => nothing to walk, propagate. */
    @Test
    public void noKeystoreCannotRetry() {
        assertFalse(MySQLProtocol.hasAnotherAliasToTry(null, false));
    }

    /** Keystore loaded and aliases exhausted => stop, surface the real error. */
    @Test
    public void exhaustedAliasesCannotRetry() {
        Enumeration<String> empty = Collections.emptyEnumeration();
        assertFalse(MySQLProtocol.hasAnotherAliasToTry(empty, true));
    }

    /** Keystore loaded with an untried alias left => retry. */
    @Test
    public void remainingAliasCanRetry() {
        Enumeration<String> more =
                Collections.enumeration(Collections.singletonList("mysql_host"));
        assertTrue(MySQLProtocol.hasAnotherAliasToTry(more, false));
    }

    // ---- requireAliasKeystorePaths: only the two PATHS are required ----------

    /** Both paths present => no exception. */
    @Test
    public void bothPathsPresentIsAccepted() throws QueryException {
        MySQLProtocol.requireAliasKeystorePaths("/etc/ks.jks", "/etc/ts.jks");
    }

    /** Missing keystore path => clear, connection-state error (not an NPE). */
    @Test
    public void missingKeyStorePathIsRejected() {
        assertRejectedWithConnectionState(null, "/etc/ts.jks");
    }

    /** Missing trust-store path => clear, connection-state error. */
    @Test
    public void missingTrustStorePathIsRejected() {
        assertRejectedWithConnectionState("/etc/ks.jks", null);
    }

    private void assertRejectedWithConnectionState(String ks, String ts) {
        try {
            MySQLProtocol.requireAliasKeystorePaths(ks, ts);
            fail("expected QueryException for missing path");
        } catch (QueryException qe) {
            assertEquals(SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    qe.getSqlState());
        }
    }

    // ---- toPassword: unset property maps to a null password -----------------

    /** A null property => null password (KeyStore.load accepts it). */
    @Test
    public void nullPasswordPropertyMapsToNull() {
        assertNull(MySQLProtocol.toPassword(null));
    }

    /** A set property => its characters. */
    @Test
    public void passwordPropertyMapsToChars() {
        assertArrayEquals("secret".toCharArray(), MySQLProtocol.toPassword("secret"));
    }
}
