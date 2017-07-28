package net.osmand.bitcoinsender.model;


import com.google.gson.annotations.SerializedName;

import java.util.List;

public class TransactionSent {
    public String txid;

    @SerializedName("from_greem_address")
    public boolean fromGreenAddress;

    public long time;
    public int confirmations;

    @SerializedName("total_amount_sent")
    public String totalAmountSent;

    public List<Amount> amountsSent;
    public List<String> senders;

    public double confidence;

    @SerializedName("propagated_by_nodes")
    public int propagatedByNodes;
}
