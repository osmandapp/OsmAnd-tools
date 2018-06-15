package net.osmand.bitcoinsender.utils;

import net.osmand.bitcoinsender.model.Input;
import net.osmand.bitcoinsender.model.Signer;
import net.osmand.bitcoinsender.model.WithdrawSignRequest;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.DERSequenceGenerator;
import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.PBEParametersGenerator;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.crypto.signers.HMacDSAKCalculator;
import org.bouncycastle.util.encoders.Base64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.Arrays;

public class SigningUtils {

    public static WithdrawSignRequest signWithdrawalRequest(WithdrawSignRequest request, String secretPin) throws BlockIOException {

        byte[] privKey = getPrivKeyFromPassphrase(
            Base64.decode(request.encryptedPassphrase.passphrase),
            pinToKey(secretPin));

        byte[] generatedPubKey = SigningUtils.derivePublicKey(privKey);
        for (Input input : request.inputs) {
            for (Signer signer : input.signers) {
                if (Arrays.equals(generatedPubKey, SigningUtils.fromHex(signer.signerPubKey))) {
                    signer.signedData = signData(input.dataToSign, privKey);
                }
            }
        }

        return request;
    }

    /**
     * Step (0) to (3): Converts secret PIN to AES key
     * @param pin Secret PIN
     * @return AES key for next steps
     */
    static byte[] pinToKey(String pin) {
        int iterations = 1024;
        byte[] pinBytes = PBEParametersGenerator.PKCS5PasswordToUTF8Bytes(pin.toCharArray());
        byte[] salt = PBEParametersGenerator.PKCS5PasswordToUTF8Bytes("".toCharArray());

        PKCS5S2ParametersGenerator generator = new PKCS5S2ParametersGenerator(new SHA256Digest());
        generator.init(pinBytes, salt, iterations);
        KeyParameter params = (KeyParameter)generator.generateDerivedParameters(128);

        byte[] intResult = PBEParametersGenerator.PKCS5PasswordToUTF8Bytes(toHex(params.getKey()).toCharArray());

        generator = new PKCS5S2ParametersGenerator(new SHA256Digest());
        generator.init(intResult, salt, iterations);
        params = (KeyParameter)generator.generateDerivedParameters(256);

        return params.getKey();
    }

    /**
     * Steps (4) to (7): Decrypt passphrase with secret key
     * @param pass passphrase from withdrawal request
     * @param key secret key
     * @return decrypted passphrase for next steps
     * @throws BlockIOException
     */
    static byte[] decryptPassphrase(byte[] pass, byte[] key) throws BlockIOException {
        PaddedBufferedBlockCipher aes = new PaddedBufferedBlockCipher(new AESEngine());
        CipherParameters aesKey = new KeyParameter(key);
        aes.init(false, aesKey);
        try {
            return cipherData(aes, pass);
        } catch (InvalidCipherTextException e) {
            throw new BlockIOException("Unexpected error while signing transaction. Please file an issue report.");
        }
    }

    /**
     * Wrapper for getting a private key from an encrypted passphrase
     * and encryption key combination
     * @param encryptedPassphrase the encrypted passphrase
     * @param key the secret key
     * @return private key
     * @throws BlockIOException
     */
    static byte[] getPrivKeyFromPassphrase(byte[] encryptedPassphrase, byte[] key) throws BlockIOException {
        byte[] passphrase = decryptPassphrase(encryptedPassphrase, key);
        try {
            return getPrivKey(fromHex(new String(passphrase, "UTF-8")));
        } catch (UnsupportedEncodingException e) {
            throw new BlockIOException("Your system does not seem to support UTF-8 encoding! Aborting signing process.");
        }
    }

    /**
     * Used only for testing: encrypt secret passphrase
     * @param plain plain passphrase
     * @param key secret key
     * @return encrypted passphrase
     * @throws BlockIOException
     */
    static byte[] encryptPassphrase(String plain, byte[] key) throws BlockIOException {
        PaddedBufferedBlockCipher aes = new PaddedBufferedBlockCipher(new AESEngine());
        CipherParameters aesKey = new KeyParameter(key);
        aes.init(true, aesKey);
        try {
            return cipherData(aes, plain.getBytes("UTF-8"));
        } catch (InvalidCipherTextException e) {
            throw new BlockIOException("Unexpected error while signing transaction. Please file an issue report.");
        } catch (UnsupportedEncodingException e) {
            throw new BlockIOException("Your system does not seem to support UTF-8 encoding! Aborting signing process.");
        }
    }

    static byte[] cipherData(PaddedBufferedBlockCipher cipher, byte[] data) throws InvalidCipherTextException {
        int minSize = cipher.getOutputSize(data.length);
        byte[] outBuf = new byte[minSize];
        int length1 = cipher.processBytes(data, 0, data.length, outBuf, 0);
        int length2 = cipher.doFinal(outBuf, length1);
        int actualLength = length1 + length2;
        byte[] result = new byte[actualLength];
        System.arraycopy(outBuf, 0, result, 0, result.length);
        return result;
    }

    /**
     * Step (8): Get privkey from decrypted passphrase
     * @param passphrase
     * @return
     */
    static byte[] getPrivKey(byte[] passphrase) {
        SHA256Digest digest = new SHA256Digest();
        byte [] privBytes = new byte[digest.getDigestSize()];
        digest.update(passphrase, 0, passphrase.length);
        digest.doFinal(privBytes, 0);
        return privBytes;
    }

    /**
     * Step (8) to (11): Derive pubkey from passphrase
     * @param privBytes
     * @return
     * @throws BlockIOException
     */
    static byte[] derivePublicKey(byte[] privBytes) throws BlockIOException {
        X9ECParameters params = SECNamedCurves.getByName("secp256k1");
        ECDomainParameters ecParams = new ECDomainParameters(params.getCurve(), params.getG(), params.getN(),  params.getH());
        BigInteger priv = new BigInteger(1, privBytes);
        byte[] pubBytes = ecParams.getG().multiply(priv).getEncoded(true);

        return pubBytes;
    }


    static String signData(String input, byte[] key) throws BlockIOException {
        ECDSASigner signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest()));
        X9ECParameters params = SECNamedCurves.getByName("secp256k1");
        ECDomainParameters ecParams = new ECDomainParameters(params.getCurve(), params.getG(), params.getN(), params.getH());
        BigInteger priv = new BigInteger(1, key);
        ECPrivateKeyParameters privKey = new ECPrivateKeyParameters(priv, ecParams);

        signer.init(true, privKey);
        BigInteger[] sigs = signer.generateSignature(fromHex(input));

        BigInteger r = sigs[0];
        BigInteger s = sigs[1];

        // BIP62: "S must be less than or equal to half of the Group Order N"
        BigInteger overTwo = params.getN().shiftRight(1);
        if (s.compareTo(overTwo) == 1) {
            s = params.getN().subtract(s);
        }

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DERSequenceGenerator seq = new DERSequenceGenerator(bos);
            seq.addObject(new ASN1Integer(r));
            seq.addObject(new ASN1Integer(s));
            seq.close();
            return toHex(bos.toByteArray());
        } catch (IOException e) {
            throw new BlockIOException("That should never happen... File an issue report.");  // Cannot happen.
        }
    }

    /**
     * Convert byte array to a hex string representation
     * @param array input bytes
     * @return hex string
     */
    static String toHex(byte[] array)
    {
        BigInteger bi = new BigInteger(1, array);
        String hex = bi.toString(16);
        int paddingLength = (array.length * 2) - hex.length();
        if(paddingLength > 0) {
            return String.format("%0"  +paddingLength + "d", 0) + hex;
        } else {
            return hex;
        }
    }

    /**
     * Converts a hex string representation of bytes into a byte array
     * @param s hex string
     * @return byte array
     */
    static byte[] fromHex(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
}