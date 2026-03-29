import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import javax.crypto.Cipher;

public class RSAUtils {

    private static final String ALGORITHM = "RSA";
    private static final String OAEP_SHA256 = "RSA/ECB/OAEPWithSHA-256AndMGF1Padding";
    private static final String PKCS1 = "RSA/ECB/PKCS1Padding";

    public static KeyPair generateKeyPair() throws Exception {
        return generateKeyPair(2048);
    }

    public static KeyPair generateKeyPair(int keySize) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance(ALGORITHM);
        generator.initialize(keySize);
        return generator.generateKeyPair();
    }

    public static String publicKeyToString(PublicKey key) {
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    public static String privateKeyToString(PrivateKey key) {
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }

    public static PublicKey stringToPublicKey(String key) throws Exception {
        byte[] data = Base64.getDecoder().decode(key);
        X509EncodedKeySpec spec = new X509EncodedKeySpec(data);
        KeyFactory factory = KeyFactory.getInstance(ALGORITHM);
        return factory.generatePublic(spec);
    }

    public static PrivateKey stringToPrivateKey(String key) throws Exception {
        byte[] data = Base64.getDecoder().decode(key);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(data);
        KeyFactory factory = KeyFactory.getInstance(ALGORITHM);
        return factory.generatePrivate(spec);
    }

    public static String encrypt(String plaintext, PublicKey key) throws Exception {
        byte[] encrypted = encryptBytes(plaintext.getBytes("UTF-8"), key);
        return Base64.getEncoder().encodeToString(encrypted);
    }

    public static String decrypt(String ciphertext, PrivateKey key) throws Exception {
        byte[] data = Base64.getDecoder().decode(ciphertext);
        byte[] decrypted = decryptBytes(data, key);
        return new String(decrypted, "UTF-8");
    }

    public static byte[] encryptBytes(byte[] data, PublicKey key) throws Exception {
        Cipher cipher = newCipher();
        cipher.init(Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(data);
    }

    public static byte[] decryptBytes(byte[] data, PrivateKey key) throws Exception {
        Cipher cipher = newCipher();
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(data);
    }
    
    private static Cipher newCipher() throws Exception {
        try {
            return Cipher.getInstance(OAEP_SHA256);
        } catch (Exception ignored) {
            return Cipher.getInstance(PKCS1);
        }
    }
}
