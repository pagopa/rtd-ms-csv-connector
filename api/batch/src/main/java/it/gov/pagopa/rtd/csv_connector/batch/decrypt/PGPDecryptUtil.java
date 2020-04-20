package it.gov.pagopa.rtd.csv_connector.batch.decrypt;

import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Iterator;

@NoArgsConstructor
public class PGPDecryptUtil {

    @SuppressWarnings("unchecked")
    public static PGPPublicKey readPublicKey(InputStream in) throws IOException, PGPException {

        in = org.bouncycastle.openpgp.PGPUtil.getDecoderStream(in);

        PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(in);

        PGPPublicKey key = null;

        Iterator<PGPPublicKeyRing> rIt = pgpPub.getKeyRings();

        while (key == null && rIt.hasNext()) {
            PGPPublicKeyRing kRing = rIt.next();
            Iterator<PGPPublicKey> kIt = kRing.getPublicKeys();
            while (key == null && kIt.hasNext()) {
                PGPPublicKey k = kIt.next();

                if (k.isEncryptionKey()) {
                    key = k;
                }
            }
        }

        if (key == null) {
            throw new IllegalArgumentException("Can't find encryption key in key ring.");
        }

        return key;
    }

    /**
     * Load a secret key ring collection from keyIn and find the secret key
     * corresponding to keyID if it exists.
     *
     * @param keyIn
     *            input stream representing a key ring collection.
     * @param keyID
     *            keyID we want.
     * @param pass
     *            passphrase to decrypt secret key with.
     * @return
     * @throws IOException
     * @throws PGPException
     * @throws NoSuchProviderException
     */
    private static PGPPrivateKey findSecretKey(InputStream keyIn, long keyID, char[] pass)
            throws IOException, PGPException, NoSuchProviderException {
        PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(
                org.bouncycastle.openpgp.PGPUtil.getDecoderStream(keyIn));

        PGPSecretKey pgpSecKey = pgpSec.getSecretKey(keyID);

        if (pgpSecKey == null) {
            return null;
        }

        return pgpSecKey.extractPrivateKey(pass, "BC");
    }

    /**
     * decrypt the passed in message stream
     * @throws IOException
     * @throws PGPException
     * @throws NoSuchProviderException
     */
    @SuppressWarnings("unchecked")
    public static byte[] decryptFile(InputStream in, InputStream keyIn, char[] passwd)
            throws IOException, NoSuchProviderException, PGPException {
        Security.addProvider(new BouncyCastleProvider());

        in = org.bouncycastle.openpgp.PGPUtil.getDecoderStream(in);

        PGPObjectFactory pgpF = new PGPObjectFactory(in);
        PGPEncryptedDataList enc;

        Object o = pgpF.nextObject();
        //
        // the first object might be a PGP marker packet.
        //
        if (o instanceof PGPEncryptedDataList) {
            enc = (PGPEncryptedDataList) o;
        } else {
            enc = (PGPEncryptedDataList) pgpF.nextObject();
        }

        Iterator<PGPPublicKeyEncryptedData> it = enc.getEncryptedDataObjects();
        PGPPrivateKey sKey = null;
        PGPPublicKeyEncryptedData pbe = null;

        while (sKey == null && it.hasNext()) {
            pbe = it.next();

            sKey = findSecretKey(keyIn, pbe.getKeyID(), passwd);
        }

        if (sKey == null) {
            throw new IllegalArgumentException("Secret key for message not found.");
        }

        InputStream clear = pbe.getDataStream(sKey, "BC");

        if (pbe.isIntegrityProtected()) {
            if (!pbe.verify()) {
                throw new PGPException("Message failed integrity check");
            }
        }

        PGPObjectFactory plainFact = new PGPObjectFactory(clear);

        Object message = plainFact.nextObject();

        if (message instanceof PGPCompressedData) {
            PGPCompressedData cData = (PGPCompressedData) message;
            PGPObjectFactory pgpFact = new PGPObjectFactory(cData.getDataStream());

            message = pgpFact.nextObject();
        }

        if (message instanceof PGPLiteralData) {
            PGPLiteralData ld = (PGPLiteralData) message;

            InputStream unc = ld.getInputStream();

            return IOUtils.toByteArray(unc);

        } else if (message instanceof PGPOnePassSignatureList) {
            throw new PGPException("Encrypted message contains a signed message - not literal data.");
        } else {
            throw new PGPException("Message is not a simple encrypted file - type unknown.");
        }

    }


}
