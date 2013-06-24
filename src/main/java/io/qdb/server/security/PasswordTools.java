/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.security;

import javax.xml.bind.DatatypeConverter;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * Encapsulates several password-related utility functions.
 * Cut and paste from the Grails Crypto plugin by Ricardo J. Mendez and Robert Fischer.
 */
public class PasswordTools {

    private static final SecureRandom RND = new SecureRandom();

    /**
     * Hash the password with random salt.
     */
    public static String hashPassword(String password) {
        try {
            byte[] salt = new byte[4];
            RND.nextBytes(salt);
            byte[] pwdBytes = password.getBytes("UTF8");
            byte[] hash = sha256(concatenate(pwdBytes, salt));
            return DatatypeConverter.printBase64Binary(concatenate(hash, salt));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);  // not possible really
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Does the password match the hash?
     */
    public static boolean checkPassword(String password, String passwordHash) {
        byte[] digest = DatatypeConverter.parseBase64Binary(passwordHash);
        assert digest.length == 36; // 32 byte SHA-256 hash + 4 byte salt

        byte[] hash = new byte[32];
        byte[] salt = new byte[4];
        System.arraycopy(digest, 0, hash, 0, 32);
        System.arraycopy(digest, 32, salt, 0, 4);

        try {
            return Arrays.equals(hash, sha256(concatenate(password.getBytes("UTF8"), salt)));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);  // not possible really
        }
    }

    private static byte[] concatenate(byte[] left, byte[] right) {
        byte[] b = new byte[left.length + right.length];
        System.arraycopy(left, 0, b, 0, left.length);
        System.arraycopy(right, 0, b, left.length, right.length);
        return b;
    }
}
