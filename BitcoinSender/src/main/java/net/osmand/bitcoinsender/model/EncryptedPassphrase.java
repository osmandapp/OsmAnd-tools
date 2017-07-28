package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

public class EncryptedPassphrase {
    @SerializedName("signer_address")
    public String signerAddress;

    @SerializedName("signer_public_key")
    public String signerPubKey;

    public String passphrase;
}
