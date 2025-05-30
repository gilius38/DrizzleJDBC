/**
 * Tungsten Replicator
 * Copyright (C) 2007-2015 Continuent Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *      
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Initial developer(s): Ludovic Launer
 * Contributor(s): 
 */

package org.drizzle.jdbc.internal.common;

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.X509KeyManager;

/**
 * Implements a class handle selecting multiple aliases from an X509KeyManager.
 * The author gratefully acknowledges the blog article by Alexandre Saudate for
 * providing guidance for the implementation.
 * 
 * @see <a href=
 *      "http://alesaudate.wordpress.com/2010/08/09/how-to-dynamically-select-a-certificate-alias-when-invoking-web-services/">
 *      How to dynamically select a certificate alias when invoking web
 *      services</a>
 */
public class AliasSelectorKeyManager implements X509KeyManager {
    private final static Logger logger = Logger.getLogger(AliasSelectorKeyManager.class.getName());

    private X509KeyManager sourceKeyManager = null;
    private String alias;

    public AliasSelectorKeyManager(X509KeyManager keyManager, String alias) {
        this.sourceKeyManager = keyManager;
        this.alias = alias;
        if (logger.isLoggable(Level.FINE))
            logger.fine("Trying alias " + alias);
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {

        if (this.alias == null || keyType == null)
            return sourceKeyManager.chooseClientAlias(keyType, issuers, socket);
        else {
            Set<String> validAliases = new HashSet<String>();
            // iterate through keyTypes (there will generally be only one
            for (String kt : keyType) {
                String[] aliases = sourceKeyManager.getClientAliases(kt, issuers);
                if (aliases != null)
                    validAliases.addAll(Arrays.asList(aliases));
            }
            if (validAliases.contains(alias)) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(MessageFormat.format("Alias Found !: {0} for keyType: {1} in keystore: {2}", this.alias,
                            keyType, System.getProperty("javax.net.ssl.keyStore")));
                return alias;
            }
            if (logger.isLoggable(Level.FINE))
                logger.fine(MessageFormat.format("Could not find alias: {0} in keystore: {2}", this.alias,
                        System.getProperty("javax.net.ssl.keyStore")));
        }
        return null;
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        if (this.alias == null)
            return sourceKeyManager.chooseServerAlias(keyType, issuers, socket);
        else {
            boolean aliasFound = false;

            String[] validAliases = sourceKeyManager.getClientAliases(keyType, issuers);
            if (validAliases != null) {
                for (int j = 0; j < validAliases.length && !aliasFound; j++) {
                    if (validAliases[j].equals(alias))
                        aliasFound = true;
                }
            }

            if (aliasFound) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine(MessageFormat.format("Alias Found !: {0} for keyType: {1} in keystore: {2}", this.alias,
                            keyType, System.getProperty("javax.net.ssl.keyStore")));
                return alias;
            }

            else {
                // Not finding the alias is not an error at this stage, it only
                // means that we could not find the alias for a given keyType
                // (i.e.:EC_EC), it may exist for the desired keyType (i.e.:RSA)
                if (logger.isLoggable(Level.FINE))
                    logger.fine(MessageFormat.format("Could not find alias: {0} for keyType: {1} in keystore: {2}",
                            this.alias, keyType, System.getProperty("javax.net.ssl.keyStore")));
            }
        }

        return null;
    }

    public X509Certificate[] getCertificateChain(String alias) {
        return sourceKeyManager.getCertificateChain(alias);
    }

    public String[] getClientAliases(String keyType, Principal[] issuers) {
        return sourceKeyManager.getClientAliases(keyType, issuers);
    }

    public PrivateKey getPrivateKey(String alias) {

        return sourceKeyManager.getPrivateKey(alias);
    }

    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return sourceKeyManager.getServerAliases(keyType, issuers);
    }

}