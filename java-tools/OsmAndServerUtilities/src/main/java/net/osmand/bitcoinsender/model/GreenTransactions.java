package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class GreenTransactions {
    @SerializedName("green_txs")
    public List<GreenTransaction> greenTransactions;
}
