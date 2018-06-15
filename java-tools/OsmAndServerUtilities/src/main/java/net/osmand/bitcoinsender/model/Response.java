package net.osmand.bitcoinsender.model;

import com.google.gson.annotations.SerializedName;

public class Response {
    public String status;

    public class ResponseAccountBalance extends Response {
        @SerializedName("data")
        public AccountBalance accountBalance;
    }

    public class FeeResponse extends Response {
        @SerializedName("data")
        public net.osmand.bitcoinsender.model.FeeResponse feeResponse;
    }

    public class ResponseNewAddress extends Response {
        @SerializedName("data")
        public NewAddress newAddress;
    }

    public class ResponseAccountAddresses extends Response {
        @SerializedName("data")
        public AccountAddresses accountAddresses;
    }

    public class ResponseAddressBalances extends Response {
        @SerializedName("data")
        public AddressBalances addressBalances;
    }

    public class ResponseAddressByLabel extends Response {
        @SerializedName("data")
        public AddressByLabel addressByLabel;
    }

    public class ResponseWithdrawal extends Response {
        @SerializedName("data")
        public Withdrawal withdrawal;
    }

    public class ResponseWithdrawSignRequest extends Response {
        @SerializedName("data")
        public WithdrawSignRequest withdrawSignRequest;
    }

    public class ResponsePrices extends Response {
        @SerializedName("data")
        public Prices prices;
    }

    public class ResponseGreenAddresses extends Response {
        @SerializedName("data")
        public GreenAddresses greenAddresses;
    }

    public class ResponseGreenTransactions extends Response {
        @SerializedName("data")
        public GreenTransactions greenTransactions;
    }

    public class ResponseTransactionsReceived extends Response {
        @SerializedName("data")
        public TransactionsReceived transactionsReceived;
    }

    public class ResponseTransactionsSent extends Response {
        @SerializedName("data")
        public TransactionsSent transactionsSent;
    }

    public class ResponseError extends Response {
        @SerializedName("data")
        public Error error;
    }
}
