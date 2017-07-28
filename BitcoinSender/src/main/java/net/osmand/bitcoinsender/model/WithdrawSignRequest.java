package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class WithdrawSignRequest {
    @SerializedName("reference_id")
    public String referenceID;

    @SerializedName("more_signatures_needed")
    public boolean moreSigsNeeded;

    public List<Input> inputs;

    @SerializedName("encrypted_passphrase")
    public EncryptedPassphrase encryptedPassphrase;
}
