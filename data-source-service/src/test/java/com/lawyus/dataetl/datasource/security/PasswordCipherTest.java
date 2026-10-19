package com.lawyus.dataetl.datasource.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PasswordCipherTest {

    private static PasswordCipher cipher(String secret) throws Exception {
        PasswordCipher cipher = new PasswordCipher();
        Field field = PasswordCipher.class.getDeclaredField("secretKey");
        field.setAccessible(true);
        field.set(cipher, secret);
        cipher.init();
        return cipher;
    }

    @Test
    @DisplayName("配置密钥后加解密往返，存储为 ENC(v1:...)")
    void encryptDecryptRoundTrip() throws Exception {
        PasswordCipher cipher = cipher("unit-test-secret");

        String stored = cipher.encrypt("p@ssw0rd");
        assertThat(stored).startsWith("ENC(v1:").endsWith(")");
        assertThat(stored).doesNotContain("p@ssw0rd");
        assertThat(cipher.decrypt(stored)).isEqualTo("p@ssw0rd");
    }

    @Test
    @DisplayName("同一明文每次密文不同（随机 IV），但都可解密")
    void randomIvMakesCiphertextDifferent() throws Exception {
        PasswordCipher cipher = cipher("unit-test-secret");

        String a = cipher.encrypt("same");
        String b = cipher.encrypt("same");
        assertThat(a).isNotEqualTo(b);
        assertThat(cipher.decrypt(a)).isEqualTo("same");
        assertThat(cipher.decrypt(b)).isEqualTo("same");
    }

    @Test
    @DisplayName("历史明文读取原样返回；已加密值重复加密不变")
    void legacyPlaintextAndIdempotentEncrypt() throws Exception {
        PasswordCipher cipher = cipher("unit-test-secret");

        assertThat(cipher.decrypt("legacy-plain")).isEqualTo("legacy-plain");
        String once = cipher.encrypt("secret");
        assertThat(cipher.encrypt(once)).isEqualTo(once);
    }

    @Test
    @DisplayName("未配置密钥：明文存取退化，但遇到 ENC 值拒绝解密")
    void withoutKeyRefusesEncryptedValue() throws Exception {
        PasswordCipher cipher = cipher("");

        assertThat(cipher.enabled()).isFalse();
        assertThat(cipher.encrypt("plain")).isEqualTo("plain");
        assertThat(cipher.decrypt("plain")).isEqualTo("plain");
        assertThatThrownBy(() -> cipher.decrypt("ENC(v1:AAAA)"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ETL_SECRET_KEY");
    }
}
