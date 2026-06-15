/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson, Stephane Giron, Marc Isambart, Trond Norbye
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

import static org.drizzle.jdbc.internal.common.packet.buffer.WriteBuffer.intToByteArray;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;

import org.drizzle.jdbc.DrizzleResultSet;
import org.drizzle.jdbc.internal.SQLExceptionMapper;
import org.drizzle.jdbc.internal.common.AliasSelectorKeyManager;
import org.drizzle.jdbc.internal.common.BinlogDumpException;
import org.drizzle.jdbc.internal.common.ColumnInformation;
import org.drizzle.jdbc.internal.common.PacketFetcher;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.ServerStatus;
import org.drizzle.jdbc.internal.common.SupportedDatabases;
import org.drizzle.jdbc.internal.common.Utils;
import org.drizzle.jdbc.internal.common.ValueObject;
import org.drizzle.jdbc.internal.common.packet.EOFPacket;
import org.drizzle.jdbc.internal.common.packet.ErrorPacket;
import org.drizzle.jdbc.internal.common.packet.OKPacket;
import org.drizzle.jdbc.internal.common.packet.RawPacket;
import org.drizzle.jdbc.internal.common.packet.ResultPacket;
import org.drizzle.jdbc.internal.common.packet.ResultPacketFactory;
import org.drizzle.jdbc.internal.common.packet.ResultSetPacket;
import org.drizzle.jdbc.internal.common.packet.SyncPacketFetcher;
import org.drizzle.jdbc.internal.common.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.common.packet.commands.BatchStreamedQueryPacket;
import org.drizzle.jdbc.internal.common.packet.commands.ClosePacket;
import org.drizzle.jdbc.internal.common.packet.commands.SelectDBPacket;
import org.drizzle.jdbc.internal.common.packet.commands.StreamedQueryPacket;
import org.drizzle.jdbc.internal.common.query.DrizzleQuery;
import org.drizzle.jdbc.internal.common.query.Query;
import org.drizzle.jdbc.internal.common.queryresults.DrizzleQueryResult;
import org.drizzle.jdbc.internal.common.queryresults.DrizzleUpdateResult;
import org.drizzle.jdbc.internal.common.queryresults.NoSuchColumnException;
import org.drizzle.jdbc.internal.common.queryresults.QueryResult;
import org.drizzle.jdbc.internal.drizzle.packet.DrizzleRowPacket;
import org.drizzle.jdbc.internal.mysql.packet.MySQLAuthSwitchRequest;
import org.drizzle.jdbc.internal.mysql.packet.MySQLFieldPacket;
import org.drizzle.jdbc.internal.mysql.packet.MySQLGreetingReadPacket;
import org.drizzle.jdbc.internal.mysql.packet.MySQLRowPacket;
import org.drizzle.jdbc.internal.mysql.packet.commands.AbbreviatedMySQLClientAuthPacket;
import org.drizzle.jdbc.internal.mysql.packet.commands.MySQLBinlogDumpPacket;
import org.drizzle.jdbc.internal.mysql.packet.commands.MySQLClientAuthPacket;
import org.drizzle.jdbc.internal.mysql.packet.commands.MySQLPingPacket;

/**
 * TODO: refactor, clean up TODO: when should i read up the resultset? TODO:
 * thread safety? TODO: exception handling User: marcuse Date: Jan 14, 2009
 * Time: 4:06:26 PM
 */
public class MySQLProtocol implements Protocol {
    private final static Logger log = Logger.getLogger(MySQLProtocol.class.getName());
    private static final int MAX_DEFAULT_PACKET_LENGTH = 0x00FFFFFF;

    // CT-2779: keystore properties for the client-cert alias walk.
    private static final String KEYSTORE_PROP = "javax.net.ssl.keyStore";
    private static final String KEYSTORE_PASSWORD_PROP = "javax.net.ssl.keyStorePassword";
    private static final String TRUSTSTORE_PROP = "javax.net.ssl.trustStore";
    private static final String TRUSTSTORE_PASSWORD_PROP = "javax.net.ssl.trustStorePassword";

    private boolean connected = false;
    private Socket socket;
    private BufferedOutputStream writer;
    private String version;
    private boolean readOnly = false;
    private final String host;
    private final int port;
    private String database;
    private final String username;
    private final String password;
    private final List<Query> batchList;
    private long batchSize;
    private PacketFetcher packetFetcher;
    private final Properties info;
    private long serverThreadId;
    private volatile boolean queryWasCancelled = false;
    private volatile boolean queryTimedOut = false;
    private boolean hasMoreResults = false;
    private int maxAllowedPacket = MAX_DEFAULT_PACKET_LENGTH;
    private boolean ssl;
    private String mysqlPublicKey;
    private boolean allowMultiQueries;
    private boolean stripQueryComments;

    /**
     * Get a protocol instance
     *
     * @param host     the host to connect to
     * @param port     the port to connect to
     * @param database the initial database
     * @param username the username
     * @param password the password
     * @param info
     * @throws org.drizzle.jdbc.internal.common.QueryException if there is a problem
     *                                                         reading / sending the
     *                                                         packets
     */
    public MySQLProtocol(final String host, final int port, final String database, final String username,
            final String password, Properties info) throws QueryException {
        this.info = info;
        this.host = host;
        this.port = port;
        this.database = (database == null ? "" : database);
        this.username = (username == null ? "" : username);
        this.password = (password == null ? "" : password);
        batchList = new ArrayList<Query>();
        batchSize = 0;
        byte packetSeq = 1;
        MySQLGreetingReadPacket greetingPacket = null;
        Set<MySQLServerCapabilities> capabilities = null;
        SSLSocketFactory sslSocketFactory = null;
        KeyStore keyStore = null;
        KeyManager[] keyManagers = null;
        TrustManager[] trustManagers = null;
        SSLContext context = null;
        Enumeration<String> aliases = null;
        boolean retry = false;
        String previousAlias = null;

        // CT-2779: keystore / trust-store properties for the alias walk. They do
        // not change during a connect, so read them once here instead of on every
        // retry iteration and every (incl. non-cert) auth failure.
        final String keyStorePath = System.getProperty(KEYSTORE_PROP);
        final String trustStorePath = System.getProperty(TRUSTSTORE_PROP);
        final char[] keyStorePassword = toPassword(System.getProperty(KEYSTORE_PASSWORD_PROP));
        final char[] trustStorePassword = toPassword(System.getProperty(TRUSTSTORE_PASSWORD_PROP));
        final boolean keystoreConfigured = keyStorePath != null;

        // Provide a way to enable a given set of SSL protocols through
        // comma-no-space list in the URL parameter
        String[] enabledProtocols = new String[] { "TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1" };
        if (info.getProperty("enabledProtocols") != null) {
            // no additional check: it's of user's responsibility to
            // provide a well formed list
            enabledProtocols = info.getProperty("enabledProtocols").split(",");
        }

        // CT-2779: close the socket on any throw that aborts the constructor.
        boolean connectComplete = false;
        try {
            do {
                packetSeq = 1;
                greetingPacket = plainConnect();
                capabilities = extractInfosFromGreetingPacket(greetingPacket);
                if (capabilities.contains(MySQLServerCapabilities.SSL)) {
                    ssl = true;
                } else if (info.getProperty("useSSL") != null) {
                    throw new QueryException("Trying to connect with ssl, but ssl not enabled in the server");
                } else {
                    ssl = false;
                }
                if (ssl) {
                    try {
                        packetSeq++;
                        AbbreviatedMySQLClientAuthPacket amcap = new AbbreviatedMySQLClientAuthPacket(capabilities);
                        amcap.send(writer);
                        boolean aliasWorks = false;
                        if (sslConnectSuccessful(sslSocketFactory, enabledProtocols)) {
                            // CT-2779: under TLS 1.3 a rejected client cert isn't
                            // reported by startHandshake() - it breaks the first
                            // auth exchange. Run it here so a transport break tries
                            // the next alias; a real auth/DB error is rethrown.
                            try {
                                authenticate(greetingPacket, capabilities, packetSeq);
                                aliasWorks = true;
                            } catch (RuntimeException | QueryException e) {
                                // A late TLS 1.3 cert rejection can surface as a
                                // CONNECTION_EXCEPTION QueryException (transport
                                // break) OR, when the server tears the connection
                                // mid auth-plugin exchange, as a RuntimeException
                                // (truncated-packet parse / plugin error). Retry
                                // the next alias for either; a real auth/DB error
                                // (other SQL state) is surfaced.
                                if (!(isRetryableCertFailure(e) && hasAnotherAliasToTry(
                                        aliases, keystoreConfigured)))
                                    throw e;
                                if (log.isLoggable(Level.FINE))
                                    log.fine("Auth broke right after the TLS handshake"
                                            + (sslSocketFactory == null
                                                    ? " (default alias)"
                                                    : " (alias " + previousAlias + ")")
                                            + "; treating as a rejected client"
                                            + " certificate, trying the next alias");
                            }
                        }
                        if (aliasWorks) {
                            if (log.isLoggable(Level.FINE) && sslSocketFactory == null)
                                log.fine("First alias succeeded");
                            retry = false;
                        }
                        else {
                            // close this socket; plainConnect() opens a fresh one
                            closeSocketQuietly();
                            // load keystore to get the aliases, only once
                            if (keyStore == null) {
                                // CT-2779: clear error instead of FileInputStream(null) NPE
                                requireAliasKeystorePaths(keyStorePath, trustStorePath);
                                try {
                                    // both key and trust stores will be needed to init the ssl context
                                    KeyManagerFactory kmFact = KeyManagerFactory
                                            .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                                    keyStore = KeyStore.getInstance("jks");
                                    // CT-2779: try-with-resources so the keystore
                                    // and truststore streams are closed (no FD leak)
                                    try (FileInputStream fis = new FileInputStream(keyStorePath)) {
                                        keyStore.load(fis, keyStorePassword);
                                    }
                                    kmFact.init(keyStore, keyStorePassword);
                                    keyManagers = kmFact.getKeyManagers();
                                    TrustManagerFactory tmFact = TrustManagerFactory
                                            .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                                    KeyStore ts = KeyStore.getInstance("jks");
                                    try (FileInputStream tfis = new FileInputStream(trustStorePath)) {
                                        ts.load(tfis, trustStorePassword);
                                    }
                                    tmFact.init(ts);
                                    trustManagers = tmFact.getTrustManagers();
                                    aliases = keyStore.aliases();
                                    if (!aliases.hasMoreElements()) {
                                        throw new QueryException(
                                                "Could not connect, did not find additional aliases to try");
                                    }
                                } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException
                                        | UnrecoverableKeyException | IOException e) {
                                    throw new QueryException(
                                            "Could not connect - error loading keystores to read aliases" + e.getMessage(),
                                            -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                                }
                            }
                            String aliasBeingTried = null;
                            try {
                                if (!aliases.hasMoreElements()) {
                                    throw new QueryException("Could not connect, all keystore aliases found have already been tried");
                                }
                                aliasBeingTried = aliases.nextElement();
                                retry = true;
                                previousAlias = aliasBeingTried;
                                // that's where we force the use of our home-made alias selector
                                for (int i = 0; i < keyManagers.length; i++) {
                                    if (keyManagers[i] instanceof X509KeyManager) {
                                        keyManagers[i] = new AliasSelectorKeyManager((X509KeyManager) keyManagers[i],
                                                aliasBeingTried);
                                    }
                                }
                                // Use the first protocol in SSLContext - the fact that there may be
                                // multiple configured protocols is not handled here;
                                // SSLContext.getInstance only takes one
                                context = SSLContext.getInstance(enabledProtocols[0]);
                                context.init(keyManagers, trustManagers, null);
                                sslSocketFactory = context.getSocketFactory();
                            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                                throw new QueryException("Could not load key store "
                                        + keyStorePath + e.getLocalizedMessage());
                            }
                        } /* !sslConnectSuccessful */
                    } catch (IOException e) {
                        throw new QueryException("Could not connect: " + e.getMessage(), -1,
                                SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                    }
                } /* if ssl */
            } while (retry);
            // CT-2779: the SSL path authenticated inside the loop; the non-SSL
            // path does it here. finishConnect() runs once, outside the retry scope.
            if (!ssl)
                authenticate(greetingPacket, capabilities, packetSeq);
            finishConnect();
            connectComplete = true;
        } finally {
            // any throw leaves the socket open and the caller gets no object to
            // close() - release the FD here so it is not leaked.
            if (!connectComplete)
                closeSocketQuietly();
        }
    }

    /**
     * CT-2779: close the streams and socket, ignoring errors and sending no
     * COM_QUIT (the socket may be broken). Frees the FD when the constructor
     * throws and the caller never gets an object to {@link #close()}.
     */
    private void closeSocketQuietly() {
        try { if (writer != null) writer.close(); } catch (Exception ignored) { }
        try { if (packetFetcher != null) packetFetcher.close(); } catch (Exception ignored) { }
        try { if (socket != null) socket.close(); } catch (Exception ignored) { }
    }

    /**
     * CT-2779: should a post-handshake auth failure be retried on the next
     * keystore alias? A late TLS 1.3 client-cert rejection isn't reported by
     * {@code startHandshake()}; it breaks the transport and surfaces either as a
     * {@code CONNECTION_EXCEPTION} {@link QueryException} (a broken pipe / late
     * alert funnelled through {@code authenticate()}'s {@code catch (IOException)}
     * - see {@link #isLateCertRejection}) OR, when the server tears the
     * connection mid caching_sha2/sha256 plugin exchange, as a
     * {@link RuntimeException} (truncated-packet parse / plugin error). Both can
     * be fixed by trying a different alias. A genuine auth/DB error is a
     * {@code QueryException} with another SQL state (e.g. an access-denied ERROR
     * packet) and is NOT retried - another cert cannot fix it.
     *
     * @param t the throwable from the post-handshake auth exchange
     * @return true if a different alias might fix it
     */
    static boolean isRetryableCertFailure(final Throwable t) {
        if (t instanceof QueryException)
            return isLateCertRejection((QueryException) t);
        return t instanceof RuntimeException;
    }

    /**
     * CT-2779: is a {@link QueryException} from the post-handshake auth exchange
     * a connection-level break (which a different keystore alias might fix)?
     * {@code authenticate()}'s {@code catch (IOException)} tags transport breaks
     * with the {@code CONNECTION_EXCEPTION} ("08") SQL state; a real auth error
     * carries another state.
     *
     * @param qe the exception thrown by the post-handshake auth exchange
     * @return true if it is a connection-level break
     */
    static boolean isLateCertRejection(final QueryException qe) {
        // exact "08" only (not the whole family) keeps the retry narrow;
        // authenticate() always stamps exactly that on transport breaks
        return SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState()
                .equals(qe.getSqlState());
    }

    /**
     * CT-2779: is there another keystore alias to try after a cert rejection?
     * Before the keystore is loaded we only know whether one is configured;
     * after, the alias enumeration is authoritative. False lets the original
     * rejection propagate instead of a generic "all aliases tried".
     *
     * @param aliases            the keystore alias enumeration, or null if the
     *                           keystore has not been loaded yet
     * @param keystoreConfigured whether a keystore is configured at all
     * @return true if a further alias attempt is possible
     */
    static boolean hasAnotherAliasToTry(final Enumeration<String> aliases,
            final boolean keystoreConfigured) {
        return aliases == null ? keystoreConfigured : aliases.hasMoreElements();
    }

    /**
     * CT-2779: require the keystore and trust-store PATH properties for the alias
     * walk (a null path is an uncatchable {@code FileInputStream(null)} NPE).
     * Passwords are not required here - see {@link #toPassword(String)}.
     *
     * @throws QueryException if either path property is unset
     */
    static void requireAliasKeystorePaths(final String keyStorePath,
            final String trustStorePath) throws QueryException {
        if (keyStorePath == null || trustStorePath == null) {
            throw new QueryException("Could not connect - cannot try additional"
                    + " client-certificate aliases: the " + KEYSTORE_PROP + " and "
                    + TRUSTSTORE_PROP + " system properties must both be set",
                    -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(),
                    null);
        }
    }

    /**
     * CT-2779: keystore password property to char[], mapping unset to
     * {@code null}. The JSSE load/init APIs accept a null password (truststores
     * often have none); a wrong-but-required password surfaces from the load.
     */
    static char[] toPassword(final String password) {
        return password == null ? null : password.toCharArray();
    }

    public final MySQLGreetingReadPacket plainConnect() throws QueryException {
        Integer connectTimeoutInSecs = null;
        final SocketFactory socketFactory = SocketFactory.getDefault();
        MySQLGreetingReadPacket greetingPacket = null;
        try {
            // Extract connectTimeout URL parameter
            String connectTimeoutString = info.getProperty("connectTimeout");
            if (connectTimeoutString != null) {
                try {
                    connectTimeoutInSecs = Integer.valueOf(connectTimeoutString);
                } catch (Exception e) {
                    connectTimeoutInSecs = null;
                }
            }

            // Create socket with timeout if required
            InetSocketAddress sockAddr = new InetSocketAddress(host, port);
            socket = socketFactory.createSocket();
            socket.setTcpNoDelay(true);
            if (connectTimeoutInSecs != null) {
                socket.connect(sockAddr, connectTimeoutInSecs * 1000);
            } else {
                socket.connect(sockAddr);
            }
        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        try {
            // Avoid hanging when reading welcome packet from unresponsive MySQL server,
            // using identical value as connectTimeout
            if (connectTimeoutInSecs != null)
                socket.setSoTimeout(connectTimeoutInSecs * 1000);
            BufferedInputStream reader = new BufferedInputStream(socket.getInputStream(), 32768);
            packetFetcher = new SyncPacketFetcher(reader);
            writer = new BufferedOutputStream(socket.getOutputStream(), 32768);

            // Proxy protocol v1 implementation according to spec found at:
            // http://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
            // Enabling and passing proxy protocol parameters is done through connection
            // properties
            if (Boolean.valueOf(info.getProperty("proxyProtocol"))) {
                StringBuilder proxyProtocolHeader = new StringBuilder("PROXY ")
                        .append(info.getProperty("proxyProtocol.tcpVersion")).append(" ")
                        .append(info.getProperty("proxyProtocol.clientAddress")).append(" ")
                        .append(info.getProperty("proxyProtocol.connectedToIPAddress")).append(" ")
                        .append(info.getProperty("proxyProtocol.clientPort")).append(" ")
                        .append(info.getProperty("proxyProtocol.localPort")).append("\r\n");
                if (log.isLoggable(Level.FINEST))
                    log.finest("Sending proxy protocol header: " + proxyProtocolHeader.toString());
                writer.write(proxyProtocolHeader.toString().getBytes(Charset.forName("ASCII")));
            }

            greetingPacket = new MySQLGreetingReadPacket(packetFetcher.getRawPacket());
            this.serverThreadId = greetingPacket.getServerThreadID();

            this.version = greetingPacket.getServerVersion();

        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        } finally {
            try {
                // reset so timeout so it wont affect further operations
                if (connectTimeoutInSecs != null)
                    socket.setSoTimeout(0);
            } catch (SocketException ignored) {
            }
        }
        return greetingPacket;
    }

    public final Set<MySQLServerCapabilities> extractInfosFromGreetingPacket(
            final MySQLGreetingReadPacket greetingPacket) {
        final Set<MySQLServerCapabilities> capabilities = EnumSet.of(MySQLServerCapabilities.LONG_PASSWORD,
                MySQLServerCapabilities.IGNORE_SPACE, MySQLServerCapabilities.CLIENT_PROTOCOL_41,
                MySQLServerCapabilities.TRANSACTIONS, MySQLServerCapabilities.SECURE_CONNECTION,
                MySQLServerCapabilities.LOCAL_FILES);
        if (greetingPacket.getServerCapabilities().contains(MySQLServerCapabilities.CLIENT_PLUGIN_AUTH)) {
            capabilities.add(MySQLServerCapabilities.CLIENT_PLUGIN_AUTH);
        }

        this.allowMultiQueries = Boolean.valueOf(info.getProperty("allowMultiQueries"));
        if (this.allowMultiQueries) {
            capabilities.add(MySQLServerCapabilities.MULTI_STATEMENTS);
            capabilities.add(MySQLServerCapabilities.MULTI_RESULTS);
        }
        // Enable / Disable comments stripping (enabled by default), which
        // occurs with prepared statements
        if (info.getProperty("stripQueryComments") == null) {
            // Default value is to strip comments
            this.stripQueryComments = true;
        } else
            this.stripQueryComments = Boolean.valueOf(info.getProperty("stripQueryComments"));
        if (Boolean.valueOf(info.getProperty("useSSL"))
                && greetingPacket.getServerCapabilities().contains(MySQLServerCapabilities.SSL)) {
            capabilities.add(MySQLServerCapabilities.SSL);
        }

        // If a database is given, but createDB is not defined or is false,
        // then just try to connect to the given database
        if (this.database != null && !this.database.equals("") && !createDB())
            capabilities.add(MySQLServerCapabilities.CONNECT_WITH_DB);
        if (info.getProperty("useAffectedRows", "false").equals("false")) {
            capabilities.add(MySQLServerCapabilities.FOUND_ROWS);
        }
        return capabilities;
    }

    /**
     * Attempts an SSL negotiation with the given factory.<br>
     * If the factory is null, one will be created, optionally from a server
     * certificate if passed through environment property.<br>
     * If the handshake fails with a SocketException, it might be that the alias
     * selected in the given factory is not right one. In that case, this function
     * will return false to request a retry with another alias
     * 
     * @param sslSocketFactory
     * @return false to indicate the handshake failed and that another try with
     *         another alias should be attempted
     * @throws IOException
     * @throws QueryException
     */
    public boolean sslConnectSuccessful(SSLSocketFactory sslSocketFactory, String[] enabledProtocols)
            throws IOException, QueryException {
        if (sslSocketFactory == null && info.getProperty("serverCertificate") != null) {
            File certificate = new File(info.getProperty("serverCertificate"));
            try {
                sslSocketFactory = createSSLSocketFactoryFromCertificate(certificate);
            } catch (GeneralSecurityException e) {
                throw new QueryException("Could not connect: " + e.getMessage(), -1,
                        SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        } else if (sslSocketFactory == null)
            sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();

        SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(socket,
                socket.getInetAddress().getHostAddress(), socket.getPort(), true);
        sslSocket.setTcpNoDelay(true);

        sslSocket.setEnabledProtocols(enabledProtocols);

        // Provide a way to override the default enabled cipher suites
        if (info.getProperty("enabledCipherSuites") != null) {
            sslSocket.setEnabledCipherSuites(info.getProperty("enabledCipherSuites").split(","));
        }

        sslSocket.setUseClientMode(true);
        try {
            sslSocket.startHandshake();
        } catch (SocketException | SSLHandshakeException e) {
            return false;
        }
        socket = sslSocket;
        writer = new BufferedOutputStream(socket.getOutputStream(), 32768);
        writer.flush();
        BufferedInputStream reader = new BufferedInputStream(socket.getInputStream(), 32768);
        packetFetcher = new SyncPacketFetcher(reader);
        return true;
    }

    public void authenticate(final MySQLGreetingReadPacket greetingPacket,
            final Set<MySQLServerCapabilities> capabilities, final byte packetSeq) throws QueryException {
        try {
            String mysqlPublicKeyPath = info.getProperty("serverPublicKey");
            if (mysqlPublicKeyPath != null) {
                File file = new File(mysqlPublicKeyPath);
                if (file.exists() && file.canRead()) {
                    mysqlPublicKey = new String(Files.readAllBytes(Paths.get(file.getPath())));
                } else
                    mysqlPublicKey = null;
            } else
                mysqlPublicKey = null;
            if (log.isLoggable(Level.FINEST))
                log.finest("Sending auth packet");
            MySQLClientAuthPacket cap = new MySQLClientAuthPacket(this.username, this.password, this.database,
                    capabilities, greetingPacket.getSeed(), packetSeq, greetingPacket.getAuthPlugin(), ssl);

            cap.send(writer);

            RawPacket rp = packetFetcher.getRawPacket();

            if ((rp.getByteBuffer().get(0) & 0xFF) == 0xFE) {
                // Server asking for different authentication
                final MySQLAuthSwitchRequest authSwitchPacket = new MySQLAuthSwitchRequest(rp, greetingPacket);
                AuthPlugin plugin = AuthPluginFactory.getAuthPlugin(authSwitchPacket);
                rp = plugin.authenticate(this);
            } else if ((rp.getByteBuffer().get(0) & 0xFF) == 0x01) {
                // AuthMoreData
                rp = AuthPluginFactory.getAuthPlugin(greetingPacket.getAuthPlugin(), greetingPacket.getSeed())
                        .readAuthMoreData(rp, this);
            }

            final ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rp);
            if (resultPacket.getResultType() == ResultPacket.ResultType.ERROR) {
                final ErrorPacket ep = (ErrorPacket) resultPacket;
                final String message = ep.getMessage();
                throw new QueryException("Could not connect: " + message);
            }

            // CT-2779: cert + credentials accepted here. Post-acceptance setup
            // is in finishConnect(), outside the retry loop, so a failure there
            // is a real error, not a bogus alias walk.
        } catch (IOException e) {
            throw new QueryException("Could not connect: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    /**
     * CT-2779: post-authentication setup, run once after the alias-retry loop.
     * The cert and credentials are already accepted, so a failure here is a real
     * error that must propagate - not a cert rejection to retry. That is why it
     * lives outside {@code authenticate()} and outside the loop.
     */
    private void finishConnect() throws QueryException {
        // Create the target database if requested, then switch to it
        if (createDB()) {
            executeQuery(new DrizzleQuery("CREATE DATABASE IF NOT EXISTS `" + this.database + "`"));
            executeQuery(new DrizzleQuery("USE `" + this.database + "`"));
        }
        // Read max_allowed_packet from server
        getMaxAllowedPacket();
        connected = true;
    }

    private static SSLSocketFactory createSSLSocketFactoryFromCertificate(File crtFile)
            throws GeneralSecurityException, IOException {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        // Read the certificate from disk
        X509Certificate result;
        InputStream input = new FileInputStream(crtFile);
        result = (X509Certificate) CertificateFactory.getInstance("X509").generateCertificate(input);

        // Add it to the trust store
        trustStore.setCertificateEntry(crtFile.getName(), result);

        // Convert the trust store to trust managers
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        TrustManager[] trustManagers = tmf.getTrustManagers();

        sslContext.init(null, trustManagers, null);
        return sslContext.getSocketFactory();
    }

    /**
     * Closes socket and stream readers/writers
     *
     * @throws org.drizzle.jdbc.internal.common.QueryException if the socket or
     *                                                         readers/writes cannot
     *                                                         be closed
     */
    public void close() throws QueryException {
        try {
            if (!(socket instanceof SSLSocket))
                socket.shutdownInput();
        } catch (IOException ignored) {
        }
        try {
            final ClosePacket closePacket = new ClosePacket();
            closePacket.send(writer);
            if (!(socket instanceof SSLSocket)) {
                socket.shutdownOutput();
                writer.close();
                packetFetcher.close();
            }
        } catch (IOException e) {
            throw new QueryException("Could not close connection: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        } finally {
            try {
                this.connected = false;
                socket.close();
            } catch (IOException e) {
                log.warning("Could not close socket");
            }
        }
        this.connected = false;
    }

    /**
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return !this.connected;
    }

    /**
     * create a DrizzleQueryResult - precondition is that a result set packet has
     * been read
     *
     * @param packet the result set packet from the server
     * @return a DrizzleQueryResult
     * @throws java.io.IOException when something goes wrong while reading/writing
     *                             from the server
     */
    private QueryResult createDrizzleQueryResult(final ResultSetPacket packet) throws IOException, QueryException {
        final List<ColumnInformation> columnInformation = new ArrayList<ColumnInformation>();
        for (int i = 0; i < packet.getFieldCount(); i++) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            final ColumnInformation columnInfo = MySQLFieldPacket.columnInformationFactory(rawPacket);
            columnInformation.add(columnInfo);
        }
        packetFetcher.getRawPacket();
        final List<List<ValueObject>> valueObjects = new ArrayList<List<ValueObject>>();

        while (true) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();

            if (ReadUtil.isErrorPacket(rawPacket)) {
                ErrorPacket errorPacket = (ErrorPacket) ResultPacketFactory.createResultPacket(rawPacket);
                checkIfCancelled();
                throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(),
                        errorPacket.getSqlState());
            }

            if (ReadUtil.eofIsNext(rawPacket)) {
                final EOFPacket eofPacket = (EOFPacket) ResultPacketFactory.createResultPacket(rawPacket);
                this.hasMoreResults = eofPacket.getStatusFlags()
                        .contains(EOFPacket.ServerStatus.SERVER_MORE_RESULTS_EXISTS);
                checkIfCancelled();

                return new DrizzleQueryResult(columnInformation, valueObjects, eofPacket.getWarningCount());
            }

            if (getDatabaseType() == SupportedDatabases.MYSQL
                    || getDatabaseType() == SupportedDatabases.MARIADB) {
                final MySQLRowPacket rowPacket = new MySQLRowPacket(rawPacket, columnInformation);
                valueObjects.add(rowPacket.getRow(packetFetcher));
            } else {
                final DrizzleRowPacket rowPacket = new DrizzleRowPacket(rawPacket, columnInformation);
                valueObjects.add(rowPacket.getRow());
            }
        }
    }

    // Based on createDrizzleQueryResult
    private boolean existsMoreResultsAfterResultset(final ResultSetPacket packet) throws IOException, QueryException {
        // Skip columns definition
        for (int i = 0; i < packet.getFieldCount(); i++) {
            packetFetcher.getRawPacket();
        }
        packetFetcher.getRawPacket();
        while (true) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            if (ReadUtil.isErrorPacket(rawPacket)) {
                ErrorPacket errorPacket = (ErrorPacket) ResultPacketFactory.createResultPacket(rawPacket);
                checkIfCancelled();
                throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(),
                        errorPacket.getSqlState());
            }

            if (ReadUtil.eofIsNext(rawPacket)) {
                final EOFPacket eofPacket = (EOFPacket) ResultPacketFactory.createResultPacket(rawPacket);
                boolean hasMoreResults = eofPacket.getStatusFlags()
                        .contains(EOFPacket.ServerStatus.SERVER_MORE_RESULTS_EXISTS);
                checkIfCancelled();
                return hasMoreResults;
            }
            // else, normal row packet... just skip, as we don't care here
        }
    }

    private void checkIfCancelled() throws QueryException {
        if (queryWasCancelled) {
            queryWasCancelled = false;
            throw new QueryException("Query was cancelled by another thread", (short) -1, "JZ0001");
        }
        if (queryTimedOut) {
            queryTimedOut = false;
            throw new QueryException("Query timed out", (short) -1, "JZ0002");
        }
    }

    public void selectDB(final String database) throws QueryException {
        if (log.isLoggable(Level.FINEST))
            log.finest("Selecting db " + database);
        final SelectDBPacket packet = new SelectDBPacket(database);
        try {
            packet.send(writer);
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacketFactory.createResultPacket(rawPacket);
        } catch (IOException e) {
            throw new QueryException("Could not select database: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        this.database = database;
    }

    public String getServerVersion() {
        return version;
    }

    public void setReadonly(final boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean getReadonly() {
        return readOnly;
    }

    public void commit() throws QueryException {
        if (log.isLoggable(Level.FINEST))
            log.finest("commiting transaction");
        executeQuery(new DrizzleQuery("COMMIT"));
    }

    public void rollback() throws QueryException {
        if (log.isLoggable(Level.FINEST))
            log.finest("rolling transaction back");
        executeQuery(new DrizzleQuery("ROLLBACK"));
    }

    public void rollback(final String savepoint) throws QueryException {
        if (log.isLoggable(Level.FINEST))
            log.finest("rolling back to savepoint " + savepoint);
        executeQuery(new DrizzleQuery("ROLLBACK TO SAVEPOINT " + savepoint));
    }

    public void setSavepoint(final String savepoint) throws QueryException {
        executeQuery(new DrizzleQuery("SAVEPOINT " + savepoint));
    }

    public void releaseSavepoint(final String savepoint) throws QueryException {
        executeQuery(new DrizzleQuery("RELEASE SAVEPOINT " + savepoint));
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean ping() throws QueryException {
        final MySQLPingPacket pingPacket = new MySQLPingPacket();
        try {
            pingPacket.send(writer);
            if (log.isLoggable(Level.FINEST))
                log.finest("Sent ping packet");
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            return ResultPacketFactory.createResultPacket(rawPacket).getResultType() == ResultPacket.ResultType.OK;
        } catch (IOException e) {
            throw new QueryException("Could not ping: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
    }

    public QueryResult executeQuery(final Query dQuery) throws QueryException {
        if (log.isLoggable(Level.FINEST))
            log.finest("Executing streamed query: " + dQuery);
        this.hasMoreResults = false;
        final StreamedQueryPacket packet = new StreamedQueryPacket(dQuery, maxAllowedPacket);

        try {
            // make sure we are in a good state
            packetFetcher.clearInputStream();
            packet.send(writer);
        } catch (IOException e) {
            if (dQuery.length() > maxAllowedPacket) {
                throw new QueryException(
                        "Could not send query: " + e.getMessage() + ". Packet size (" + dQuery.length()
                                + ") is greater than the server max allowed packet.",
                        -1, SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
            } else
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        final RawPacket rawPacket;
        final ResultPacket resultPacket;
        try {
            rawPacket = packetFetcher.getRawPacket();
            resultPacket = ResultPacketFactory.createResultPacket(rawPacket);
        } catch (IOException e) {
            throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        switch (resultPacket.getResultType()) {
        case ERROR:
            final ErrorPacket ep = (ErrorPacket) resultPacket;
            checkIfCancelled();
            String failingQuery = dQuery.getQuery();
            log.warning("Could not execute query "
                    + (failingQuery.length() <= 1000 ? failingQuery : failingQuery.substring(0, 999) + "...") + " : "
                    + ((ErrorPacket) resultPacket).getMessage());
            throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
        case OK:
            final OKPacket okpacket = (OKPacket) resultPacket;
            this.hasMoreResults = okpacket.getServerStatus().contains(ServerStatus.MORE_RESULTS_EXISTS);
            final QueryResult updateResult = new DrizzleUpdateResult(okpacket.getAffectedRows(), okpacket.getWarnings(),
                    okpacket.getMessage(), okpacket.getInsertId());
            if (log.isLoggable(Level.FINE))
                log.fine("OK, " + okpacket.getAffectedRows());
            return updateResult;
        case RESULTSET:
            if (log.isLoggable(Level.FINE))
                log.fine("SELECT executed, fetching result set");

            try {
                return this.createDrizzleQueryResult((ResultSetPacket) resultPacket);
            } catch (IOException e) {
                throw new QueryException("Could not read result set: " + e.getMessage(), -1,
                        SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        default:
            log.severe("Could not parse result..." + resultPacket.getResultType());
            throw new QueryException("Could not parse result", (short) -1,
                    SQLExceptionMapper.SQLStates.INTERRUPTED_EXCEPTION.getSqlState());
        }
    }

    public void addToBatch(final Query dQuery) {
        batchList.add(dQuery);
        try {
            // Add query size + 1 ';' to batch size
            batchSize += dQuery.length() + 1;
        } catch (QueryException ignore) {
            // That should not happen here, this error being raised only by prepared
            // statements
        }
    }

    public int[] executeBatch() throws QueryException {
        int[] result = new int[batchList.size()];

        if (!allowMultiQueries || batchList.size() == 1) {
            // If allowMultiQueries is false or only one statement in the batch,
            // no need to bother using batching.
            // This uses the old code base.
            int i = 0;
            for (final Query query : batchList) {
                QueryResult executeQuery = executeQuery(query);
                if (executeQuery instanceof DrizzleUpdateResult) {
                    DrizzleUpdateResult update = (DrizzleUpdateResult) executeQuery;
                    result[i++] = (int) update.getUpdateCount();
                } else
                    result[i++] = Statement.SUCCESS_NO_INFO;
            }
        } else if (batchList.size() > 0) {
            // No need to try to send batch if empty
            boolean moreBatchResults = false;
            final BatchStreamedQueryPacket packet = new BatchStreamedQueryPacket(batchList, batchSize);

            try {
                // make sure we are in a good state
                packetFetcher.clearInputStream();
                packet.send(writer);
            } catch (IOException e) {
                throw new QueryException("Could not send query: " + e.getMessage(), -1,
                        SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }

            RawPacket rawPacket;
            ResultPacket resultPacket;
            int resIndex = 0;
            do {
                try {
                    rawPacket = packetFetcher.getRawPacket();
                    resultPacket = ResultPacketFactory.createResultPacket(rawPacket);
                } catch (IOException e) {
                    throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                            SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                }

                switch (resultPacket.getResultType()) {
                case ERROR:
                    // no more results after error
                    moreBatchResults = false;
                    final ErrorPacket ep = (ErrorPacket) resultPacket;
                    checkIfCancelled();
                    throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
                case OK:
                    final OKPacket okpacket = (OKPacket) resultPacket;
                    moreBatchResults = okpacket.getServerStatus().contains(ServerStatus.MORE_RESULTS_EXISTS);
                    result[resIndex++] = (int) okpacket.getAffectedRows();
                    break;
                case RESULTSET:
                    result[resIndex++] = Statement.SUCCESS_NO_INFO;
                    try {
                        moreBatchResults = existsMoreResultsAfterResultset((ResultSetPacket) resultPacket);
                    } catch (IOException e) {
                        throw new QueryException("Could not read result set: " + e.getMessage(), -1,
                                SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
                    }
                    break;
                default:
                    log.severe("Could not parse result..." + resultPacket.getResultType());
                    throw new QueryException("Could not parse result", (short) -1,
                            SQLExceptionMapper.SQLStates.INTERRUPTED_EXCEPTION.getSqlState());
                }

            } while (moreBatchResults);
        }
        clearBatch();
        return result;

    }

    public void clearBatch() {
        batchList.clear();
        batchSize = 0;
    }

    public List<RawPacket> startBinlogDump(final int startPos, final String filename) throws BinlogDumpException {
        final MySQLBinlogDumpPacket mbdp = new MySQLBinlogDumpPacket(startPos, filename);
        try {
            mbdp.send(writer);
            final List<RawPacket> rpList = new LinkedList<RawPacket>();
            while (true) {
                final RawPacket rp = this.packetFetcher.getRawPacket();
                if (ReadUtil.eofIsNext(rp)) {
                    return rpList;
                }
                rpList.add(rp);
            }
        } catch (IOException e) {
            throw new BinlogDumpException("Could not read binlog", e);
        }
    }

    public SupportedDatabases getDatabaseType() {
        return SupportedDatabases.fromVersionString(version);
    }

    public boolean supportsPBMS() {
        return info != null && info.getProperty("enableBlobStreaming", "").equalsIgnoreCase("true");
    }

    public String getServerVariable(String variable) throws QueryException {
        DrizzleQueryResult qr = (DrizzleQueryResult) executeQuery(new DrizzleQuery("select @@" + variable));
        if (!qr.next()) {
            throw new QueryException("Could not get variable: " + variable);
        }

        try {
            String value = qr.getValueObject(0).getString();
            return value;
        } catch (NoSuchColumnException e) {
            throw new QueryException("Could not get variable: " + variable);
        }
    }

    public QueryResult executeQuery(Query dQuery, InputStream inputStream) throws QueryException {
        int packIndex = 0;
        if (hasMoreResults) {
            try {
                packetFetcher.clearInputStream();
            } catch (IOException e) {
                throw new QueryException("Could clear input stream: " + e.getMessage(), -1,
                        SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);

            }
        }
        this.hasMoreResults = false;
        if (log.isLoggable(Level.FINEST))
            log.finest("Executing streamed query: " + dQuery);
        final StreamedQueryPacket packet = new StreamedQueryPacket(dQuery, maxAllowedPacket);

        try {
            packIndex = packet.send(writer);
            packIndex++;
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        RawPacket rawPacket;
        ResultPacket resultPacket;

        try {
            rawPacket = packetFetcher.getRawPacket();
            resultPacket = ResultPacketFactory.createResultPacket(rawPacket);
        } catch (IOException e) {
            throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        if (rawPacket.getPacketSeq() != packIndex)
            throw new QueryException("Got out of order packet ", -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), null);

        switch (resultPacket.getResultType()) {
        case ERROR:
            final ErrorPacket ep = (ErrorPacket) resultPacket;
            log.warning("Could not execute query " + dQuery + ": " + ((ErrorPacket) resultPacket).getMessage());
            throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
        case OK:
            break;
        case RESULTSET:
            break;
        default:
            log.severe("Could not parse result...");
            throw new QueryException("Could not parse result");
        }

        packIndex++;
        return sendFile(dQuery, inputStream, packIndex);
    }

    /**
     * cancels the current query - clones the current protocol and executes a query
     * using the new connection
     * <p/>
     * thread safe
     *
     * @throws QueryException
     */
    public void cancelCurrentQuery() throws QueryException {
        Protocol copiedProtocol = new MySQLProtocol(host, port, database, username, password, info);
        queryWasCancelled = true;
        copiedProtocol.executeQuery(new DrizzleQuery("KILL QUERY " + serverThreadId));
        copiedProtocol.close();
    }

    public void timeOut() throws QueryException {
        Protocol copiedProtocol = new MySQLProtocol(host, port, database, username, password, info);
        queryTimedOut = true;
        copiedProtocol.executeQuery(new DrizzleQuery("KILL QUERY " + serverThreadId));
        copiedProtocol.close();

    }

    public boolean createDB() {
        return info != null && info.getProperty("createDB", "").equalsIgnoreCase("true");
    }

    public boolean noPrepStmtCache() {
        return info != null && info.getProperty("noPrepStmtCache", "").equalsIgnoreCase("true");
    }

    /**
     * Send the given file to the server starting with packet number packIndex
     *
     * @param dQuery      the query that was first issued
     * @param inputStream input stream used to read the file
     * @param packIndex   Starting index, which will be used for sending packets
     * @return the result of the query execution
     * @throws QueryException if something wrong happens
     */
    private QueryResult sendFile(Query dQuery, InputStream inputStream, int packIndex) throws QueryException {
        byte[] emptyHeader = Utils.copyWithLength(intToByteArray(0), 4);
        RawPacket rawPacket;
        ResultPacket resultPacket;

        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        ByteArrayOutputStream bOS = new ByteArrayOutputStream();

        try {
            while (true) {
                int data = bufferedInputStream.read();
                if (data == -1) {
                    // Send the last packet
                    byte[] byteHeader = Utils.copyWithLength(intToByteArray(bOS.size()), 4);

                    byteHeader[3] = (byte) packIndex;
                    // Send the packet
                    writer.write(byteHeader);
                    bOS.writeTo(writer);
                    writer.flush();
                    packIndex++;
                    break;
                }

                // Add data into buffer
                bOS.write(data);

                if (bOS.size() >= MAX_DEFAULT_PACKET_LENGTH - 1) {
                    byte[] byteHeader = Utils.copyWithLength(intToByteArray(bOS.size()), 4);
                    byteHeader[3] = (byte) packIndex;
                    // Send the packet
                    writer.write(byteHeader);

                    bOS.writeTo(writer);
                    writer.flush();
                    packIndex++;
                    bOS.reset();
                }
            }
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        try {
            emptyHeader[3] = (byte) packIndex;
            writer.write(emptyHeader);
            writer.flush();
        } catch (IOException e) {
            throw new QueryException("Could not send query: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        try {
            rawPacket = packetFetcher.getRawPacket();
            resultPacket = ResultPacketFactory.createResultPacket(rawPacket);
        } catch (IOException e) {
            throw new QueryException("Could not read resultset: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }

        switch (resultPacket.getResultType()) {
        case ERROR:
            final ErrorPacket ep = (ErrorPacket) resultPacket;
            checkIfCancelled();
            throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
        case OK:
            final OKPacket okpacket = (OKPacket) resultPacket;
            this.hasMoreResults = okpacket.getServerStatus().contains(ServerStatus.MORE_RESULTS_EXISTS);
            final QueryResult updateResult = new DrizzleUpdateResult(okpacket.getAffectedRows(), okpacket.getWarnings(),
                    okpacket.getMessage(), okpacket.getInsertId());
            if (log.isLoggable(Level.FINE))
                log.fine("OK, " + okpacket.getAffectedRows());
            return updateResult;
        case RESULTSET:
            if (log.isLoggable(Level.FINE))
                log.fine("SELECT executed, fetching result set");
            try {
                return this.createDrizzleQueryResult((ResultSetPacket) resultPacket);
            } catch (IOException e) {
                throw new QueryException("Could not read result set: " + e.getMessage(), -1,
                        SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
            }
        default:
            log.severe("Could not parse result...");
            throw new QueryException("Could not parse result");
        }
    }

    private void getMaxAllowedPacket() throws QueryException {
        // Get max_allowed_packet from server
        QueryResult result = executeQuery(new DrizzleQuery("select @@max_allowed_packet"));
        DrizzleResultSet resultset = null;
        try {
            resultset = new DrizzleResultSet(result, null, this);
            if (resultset.next())
                maxAllowedPacket = resultset.getInt(1);
        } catch (SQLException e) {
            log.warning("Failed to read max_allowed_packet variable from server + (" + e.getMessage() + ")");
            maxAllowedPacket = MAX_DEFAULT_PACKET_LENGTH;
        } finally {
            if (resultset != null)
                try {
                    resultset.close();
                } catch (SQLException ignore) {
                }
        }
    }

    public QueryResult getMoreResults() throws QueryException {
        try {
            if (!hasMoreResults)
                return null;
            RawPacket rawPacket = packetFetcher.getRawPacket();
            ResultPacket resultPacket = ResultPacketFactory.createResultPacket(rawPacket);
            switch (resultPacket.getResultType()) {
            case RESULTSET:
                return createDrizzleQueryResult((ResultSetPacket) resultPacket);
            case OK:
                OKPacket okpacket = (OKPacket) resultPacket;
                this.hasMoreResults = okpacket.getServerStatus().contains(ServerStatus.MORE_RESULTS_EXISTS);
                return new DrizzleUpdateResult(okpacket.getAffectedRows(), okpacket.getWarnings(),
                        okpacket.getMessage(), okpacket.getInsertId());
            case ERROR:
                ErrorPacket ep = (ErrorPacket) resultPacket;
                checkIfCancelled();
                throw new QueryException(ep.getMessage(), ep.getErrorNumber(), ep.getSqlState());
            default:
                // Removing code warning...
                // Other cases are probably not possible, so just ignore them
                break;
            }
        } catch (IOException e) {
            throw new QueryException("Could not read result set: " + e.getMessage(), -1,
                    SQLExceptionMapper.SQLStates.CONNECTION_EXCEPTION.getSqlState(), e);
        }
        return null;
    }

    public static String hexdump(byte[] buffer, int offset) {
        return hexdump(buffer, offset, buffer.length);
    }

    public static String hexdump(byte[] buffer, int offset, int length) {
        StringBuffer dump = new StringBuffer();
        if ((buffer.length - offset) >= length) {
            dump.append(String.format("%02x", buffer[offset]));
            for (int i = offset + 1; i < length + offset; i++) {
                dump.append("_");
                dump.append(String.format("%02x", buffer[i]));
            }
        }
        return dump.toString();
    }

    public static String hexdump(ByteBuffer bb, int offset) {
        byte[] b = new byte[bb.capacity()];
        bb.mark();
        bb.get(b);
        bb.reset();
        return hexdump(b, offset);
    }

    /**
     * Catalogs are not supported in drizzle so this is a no-op with a Drizzle
     * connection<br>
     * MySQL treats catalogs as databases. The only difference with
     * {@link MySQLProtocol#selectDB(String)} is that the catalog is switched inside
     * the connection using SQL 'USE' command
     */
    public void setCatalog(String catalog) throws QueryException
    {
        if (getDatabaseType() == SupportedDatabases.MYSQL
                || getDatabaseType() == SupportedDatabases.MARIADB)
        {
            executeQuery(new DrizzleQuery("USE `" + catalog + "`"));
            this.database = catalog;
        }
        // else (Drizzle protocol): silently ignored since drizzle does not
        // support catalogs
    }

    /**
     * Catalogs are not supported in drizzle so this will always return null with a
     * Drizzle connection<br>
     * MySQL treats catalogs as databases. This function thus returns the currently
     * selected database
     */
    public String getCatalog() throws QueryException
    {
        if (getDatabaseType() == SupportedDatabases.MYSQL
                || getDatabaseType() == SupportedDatabases.MARIADB)
        {
            return getDatabase();
        }
        // else (Drizzle protocol): retrun null since drizzle does not
        // support catalogs
        return null;
    }

    /**
     * Returns the writer value.
     * 
     * @return Returns the writer.
     */
    protected BufferedOutputStream getWriter() {
        return writer;
    }

    /**
     * Returns the packetFetcher value.
     * 
     * @return Returns the packetFetcher.
     */
    protected PacketFetcher getPacketFetcher() {
        return packetFetcher;
    }

    public boolean useSsl() {
        return ssl;
    }

    /**
     * Returns the mysqlPublicKey value.
     * 
     * @return Returns the mysqlPublicKey.
     */
    protected String getMysqlPublicKey() {
        return mysqlPublicKey;
    }

    public boolean isStripQueryComments() {
        return stripQueryComments;
    }
}
