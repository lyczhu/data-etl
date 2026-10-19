package com.lawyus.dataetl.datasource.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * 数据源口令加解密（方案 v2 WP-06 / D10）：AES/GCM，密钥来自环境变量 {@code ETL_SECRET_KEY}。
 *
 * <p>存储格式 {@code ENC(v1:<base64(iv||ciphertext)>)}；历史明文值（无前缀）在读取时原样返回，
 * 便于渐进迁移。未配置密钥时退化为明文并打 WARN（仅限本地/测试；生产必须设置环境变量）。
 */
@Component
public class PasswordCipher {

    private static final Logger log = LoggerFactory.getLogger(PasswordCipher.class);
    private static final String PREFIX = "ENC(v1:";
    private static final int IV_LEN = 12;
    private static final int TAG_BITS = 128;

    @Value("${etl.security.secret-key:}")
    private String secretKey;

    private SecretKeySpec key;
    private final SecureRandom random = new SecureRandom();

    @PostConstruct
    void init() {
        if (secretKey == null || secretKey.isBlank()) {
            log.warn("etl.security.secret-key is empty; data source passwords are stored in PLAINTEXT. "
                    + "Set ETL_SECRET_KEY before production use.");
            return;
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(secretKey.getBytes(StandardCharsets.UTF_8));
            this.key = new SecretKeySpec(digest, "AES");
        } catch (Exception e) {
            throw new IllegalStateException("init password cipher failed", e);
        }
    }

    public boolean enabled() {
        return key != null;
    }

    public String encrypt(String plaintext) {
        if (plaintext == null || plaintext.isEmpty() || !enabled() || plaintext.startsWith(PREFIX)) {
            return plaintext;
        }
        try {
            byte[] iv = new byte[IV_LEN];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            byte[] joined = new byte[iv.length + encrypted.length];
            System.arraycopy(iv, 0, joined, 0, iv.length);
            System.arraycopy(encrypted, 0, joined, iv.length, encrypted.length);
            return PREFIX + Base64.getEncoder().encodeToString(joined) + ")";
        } catch (Exception e) {
            throw new IllegalStateException("encrypt data source password failed", e);
        }
    }

    public String decrypt(String stored) {
        if (stored == null || stored.isEmpty() || !stored.startsWith(PREFIX)) {
            return stored;   // 历史明文/未加密
        }
        if (!enabled()) {
            throw new IllegalStateException("data source password is encrypted but ETL_SECRET_KEY is not set");
        }
        try {
            String body = stored.substring(PREFIX.length(), stored.length() - 1);
            byte[] joined = Base64.getDecoder().decode(body);
            byte[] iv = new byte[IV_LEN];
            System.arraycopy(joined, 0, iv, 0, IV_LEN);
            byte[] ciphertext = new byte[joined.length - IV_LEN];
            System.arraycopy(joined, IV_LEN, ciphertext, 0, ciphertext.length);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            return new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("decrypt data source password failed", e);
        }
    }
}
