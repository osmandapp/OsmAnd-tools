package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

public class Price {
    public String price;

    @SerializedName("price_base")
    public String priceBase;

    public String exchange;
    public long time;
}
