package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Input {
    @SerializedName("input_no")
    public int inputNo;

    @SerializedName("signatures_needed")
    public int sigsNeeded;

    @SerializedName("data_to_sign")
    public String dataToSign;

    public List<Signer> signers;
}
