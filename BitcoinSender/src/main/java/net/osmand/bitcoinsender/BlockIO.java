package net.osmand.bitcoinsender;

import com.google.gson.Gson;
import net.osmand.bitcoinsender.utils.BlockIOException;
import net.osmand.bitcoinsender.utils.Constants;
import net.osmand.bitcoinsender.utils.SigningUtils;
import net.osmand.bitcoinsender.model.*;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.math.RoundingMode;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;

public class BlockIO {

    private String apiKey;

    public enum ParamType{
        ADDRS, LABELS, USERIDS
    }

    public BlockIO(String apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Requests the balance of the account associated with this clients' API key
     * @return An {@link AccountBalance} object containing the balances
     * @throws BlockIOException
     */
    public AccountBalance getAccountBalance() throws BlockIOException {
        Response.ResponseAccountBalance response = (Response.ResponseAccountBalance) doApiCall(Constants.Methods.GET_ACCOUNT_BALANCE, null, Response.ResponseAccountBalance.class);
        return response.accountBalance;
    }

    /**
     * Requests the creation of a new address for the account associated with the clients' API key
     * @param label Optional label for the new address. null or "" for random label
     * @return A {@link NewAddress} object containing information about the new address
     * @throws BlockIOException
     */
    public NewAddress getNewAddress(String label) throws BlockIOException {
        HashMap<String, String> params = null;
        if (label != null && !label.equals("")) {
            params = new HashMap<String, String>(1);
            params.put(Constants.Params.LABEL, label);
        }

        Response.ResponseNewAddress response = (Response.ResponseNewAddress) doApiCall(Constants.Methods.GET_NEW_ADDRESS, params, Response.ResponseNewAddress.class);
        return response.newAddress;
    }

    /**
     * Requests a list of addresses in the account associated with this clients' API key
     * @return An {@link AccountAddresses} object containing the addresses
     * @throws BlockIOException
     */
    public AccountAddresses getAccountAddresses() throws BlockIOException {
        Response.ResponseAccountAddresses response = (Response.ResponseAccountAddresses) doApiCall(Constants.Methods.GET_MY_ADDRESSES, null, Response.ResponseAccountAddresses.class);
        return response.accountAddresses;
    }

    /**
     * Requests balance(s) of given address(es) in the account associated with this clients' API key <br>
     * Make sure that the addresses actually exist in the account or the whole call will fail
     * @param addresses A String array containing the addresses to request balances for
     * @return An {@link AddressBalances} object containing the balances
     * @throws BlockIOException
     */
    public AddressBalances getAddressBalancesByAddress(String[] addresses) throws BlockIOException {
        if (addresses.length == 0) {
            throw new IllegalArgumentException("You have to provide at least one address.");
        }

        String paramString = addresses.length == 1 ? addresses[0] : Arrays.asList(addresses).toString().replaceAll("^\\[|\\]$", "");
        HashMap<String, String> params = new HashMap<String, String>(1);
        params.put(Constants.Params.ADDRS, paramString);

        Response.ResponseAddressBalances response = (Response.ResponseAddressBalances) doApiCall(Constants.Methods.GET_ADDR_BALANCE, params, Response.ResponseAddressBalances.class);
        return response.addressBalances;
    }

    /**
     * Requests balance(s) of given label(s) in the account associated with this clients' API key <br>
     * Make sure that the labels actually exist in the account or the whole call will fail
     * @param labels A String array containing the labels to request balances for
     * @return An {@link AddressBalances} object containing the balances
     * @throws BlockIOException
     */
    public AddressBalances getAddressBalancesByLabels(String[] labels) throws BlockIOException {
        if (labels.length == 0) {
            throw new IllegalArgumentException("You have to provide at least one label.");
        }

        String paramString = labels.length == 1 ? labels[0] : Arrays.asList(labels).toString().replaceAll("^\\[|\\]$", "");
        HashMap<String, String> params = new HashMap<String, String>(1);
        params.put(Constants.Params.LABELS, paramString);

        Response.ResponseAddressBalances response = (Response.ResponseAddressBalances) doApiCall(Constants.Methods.GET_ADDR_BALANCE, params, Response.ResponseAddressBalances.class);
        return response.addressBalances;
    }

    /**
     * Requests the address with the given label from the account associated with this clients' API key
     * @param label The label for which to request the address for
     * @return An {@link AddressByLabel} object containing the address and additional info about it
     * @throws BlockIOException
     */
    public AddressByLabel getAddressByLabel(String label) throws BlockIOException {
        if (label == null || label.equals("")) {
            throw new IllegalArgumentException("You have to provide a valid label.");
        }

        HashMap<String, String> params = new HashMap<String, String>(1);
        params.put(Constants.Params.LABEL, label);

        Response.ResponseAddressByLabel response = (Response.ResponseAddressByLabel) doApiCall(Constants.Methods.GET_ADDR_BY_LABEL, params, Response.ResponseAddressByLabel.class);
        return response.addressByLabel;
    }

    /**
     * Convenience method for a simple withdrawal from the account associated with this clients' API key to the specified address
     * @param address Target address
     * @param amount Amount to withdraw
     * @param secretPin The secret PIN you set at block.io to authorize and sign the withdrawal
     * @return A {@link Withdrawal} object containing information about the sent transaction.
     * @throws BlockIOException
     */
    public Withdrawal withdrawToAddress(String address, double amount, String secretPin) throws BlockIOException {
        HashMap<String, Double> target = new HashMap<String, Double>(1);
        target.put(address, amount);
        return withdraw(null, null, target, ParamType.ADDRS, secretPin);
    }

    public void printFeeForTransaction(Map<String, Double> targetsAndAmounts) {
        String method = Constants.Methods.GET_FEE;

        HashMap<String, String> params = null;

        try {
            params = setupWithdrawalParams(targetsAndAmounts, ParamType.ADDRS);
        } catch (BlockIOException e) {
            e.printStackTrace();
        }

        try {
            Response.FeeResponse signRequestResponse = (Response.FeeResponse) doPostApiCall(method, params, Response.FeeResponse.class);
            FeeResponse resp = signRequestResponse.feeResponse;
            System.out.println("Estimated fee for the transaction: " + resp.fee + " " + resp.ntwrk);

        } catch (BlockIOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Withdraw from the account associated with this clients' API key.
     * @param sources Supply an array of sources for this withdrawal. If you set this to null, then block.io will select the sources automatically.
     *                You must not mix source types!
     * @param sourceType If you supplied a sources array, this is mandatory. Must be one of {@link BlockIO.ParamType}.
     * @param targetsAndAmounts A {@link java.util.Map} with target as key ({@link java.lang.String}) and amount as value ({@link java.lang.Double}).
     *                          Each entry will be one target of the withdrawal. Limit is 100 per withdrawal.
     *                          You must not mix target types!
     * @param targetType This is mandatory and defines what type of targets this withdrawal goes to. One of {@link BlockIO.ParamType}.
     * @param secretPin The secret PIN you set at block.io to authorize and sign the withdrawal
     * @return A {@link Withdrawal} object containing information about the sent transaction.
     * @throws BlockIOException
     */
    public Withdrawal withdraw(String[] sources, ParamType sourceType, Map<String, Double> targetsAndAmounts, ParamType targetType, String secretPin) throws BlockIOException {
        if (targetsAndAmounts == null || targetsAndAmounts.size() == 0) {
            throw new IllegalArgumentException("You have to provide between one and 100 pair(s) of targets and amounts to withdraw to");
        }

        if (secretPin == null || secretPin.equals("")) {
            throw new IllegalArgumentException("You have to provide your secret pin with withdrawals");
        }

        HashMap<String, String> params = setupWithdrawalParams(targetsAndAmounts, targetType);

        String method = Constants.Methods.WITHDRAW_FROM_ANY;
        if (sources != null && sources.length > 0) {
            String sourcesString = sources.length == 1 ? sources[0] : Arrays.asList(sources).toString().replaceAll("^\\[|\\]$", "");
            switch (sourceType) {
                case ADDRS:
                    params.put(Constants.Params.FROM_ADDRS, sourcesString);
                    method = Constants.Methods.WITHDRAW_FROM_ADDRS;
                    break;
                case LABELS:
                    params.put(Constants.Params.FROM_LABELS, sourcesString);
                    method = Constants.Methods.WITHDRAW_FROM_LABELS;
                    break;
                case USERIDS:
                    params.put(Constants.Params.FROM_USERIDS, sourcesString);
                    method = Constants.Methods.WITHDRAW_FROM_USERIDS;
                    break;
                default:
                    throw new BlockIOException("You requested a withdrawal from specific sources but did not set the source type.");
            }
        }

        Response.ResponseWithdrawSignRequest signRequestResponse = (Response.ResponseWithdrawSignRequest) doPostApiCall(method, params, Response.ResponseWithdrawSignRequest.class);
        WithdrawSignRequest signRequest = signRequestResponse.withdrawSignRequest;

        return finalizeWithdrawal(signRequest, secretPin);
    }

    private HashMap<String, String> setupWithdrawalParams(Map<String, Double> addrsAndAmounts, ParamType targetType) throws BlockIOException {
        String addrsParamString = "";
        String amountsParamString = "";
//        NumberFormat nf = NumberFormat.getNumberInstance(Locale.US); // This will force '.' as decimal separator
        DecimalFormat df = new DecimalFormat("#.########");
        df.setRoundingMode(RoundingMode.CEILING);
        for (Map.Entry<String, Double> entry: addrsAndAmounts.entrySet()) {
            addrsParamString += entry.getKey() + ",";
            amountsParamString +=  df.format(entry.getValue()) + ",";
        }

        // Now remove the trailing ','
        addrsParamString = addrsParamString.replaceAll(",$", "");
        amountsParamString = amountsParamString.replaceAll(",$", "");

        // And put it where it belongs
        HashMap<String, String> params = new HashMap<String, String>(2);
        switch (targetType) {
            case ADDRS:
                params.put(Constants.Params.TO_ADDRS, addrsParamString);
                break;
            case LABELS:
                params.put(Constants.Params.TO_LABELS, addrsParamString);
                break;
            case USERIDS:
                params.put(Constants.Params.TO_USERIDS, addrsParamString);
                break;
            default:
                throw new BlockIOException("You did not set the target type.");
        }
        params.put(Constants.Params.AMOUNTS, amountsParamString);

        return params;
    }

    private Withdrawal finalizeWithdrawal(WithdrawSignRequest signRequest, String secretPin) throws BlockIOException {
        // Now lets' do the clientside magic
        signRequest = SigningUtils.signWithdrawalRequest(signRequest, secretPin);

        // And do the actual withdrawal
        HashMap<String, String> params = new HashMap<String, String>(1);
        Gson gson = new Gson();
        params.put(Constants.Params.SIG_DATA, gson.toJson(signRequest, WithdrawSignRequest.class));
        Response.ResponseWithdrawal withdrawalResponse = (Response.ResponseWithdrawal) doPostApiCall(Constants.Methods.WITHDRAW_DO_FINAL, params, Response.ResponseWithdrawal.class);

        return withdrawalResponse.withdrawal;
    }

    /**
     * Requests prices of the currency of the account associated with this clients' API key
     * @param baseCurrency Optional base currency to return prices in. null or "" to get prices in all available base currencies
     * @return A {@link Prices} object containing price information in one or more base currency from one or more exchange
     * @throws BlockIOException
     */
    public Prices getPrices(String baseCurrency) throws BlockIOException{
        HashMap<String, String> params = null;
        if (baseCurrency != null && !baseCurrency.equals("")) {
            params = new HashMap<String, String>(1);
            params.put(Constants.Params.PRICE_BASE, baseCurrency);
        }

        Response.ResponsePrices response = (Response.ResponsePrices) doApiCall(Constants.Methods.GET_PRICES, params, Response.ResponsePrices.class);
        return response.prices;
    }

    /**
     * Checks the given address(es) for being Block.io Green Address(es)
     * @param addresses A String array containing the addresses to request status for
     * @return An {@link GreenAddresses} object containing the subset of the given addresses that are green
     * @throws BlockIOException
     */
    public GreenAddresses isGreenAddress(String[] addresses) throws BlockIOException {
        if (addresses.length == 0) {
            throw new IllegalArgumentException("You have to provide at least one address.");
        }

        String paramString = addresses.length == 1 ? addresses[0] : Arrays.asList(addresses).toString().replaceAll("^\\[|\\]$", "");
        HashMap<String, String> params = new HashMap<String, String>(1);
        params.put(Constants.Params.ADDRS, paramString);

        Response.ResponseGreenAddresses response = (Response.ResponseGreenAddresses) doApiCall(Constants.Methods.IS_GREEN_ADDR, params, Response.ResponseGreenAddresses.class);
        return response.greenAddresses;
    }

    /**
     * Checks the given transaction(s) for being sent by a Block.io Green Address
     * @return An {@link GreenTransactions} object containing the subset of the given transactions that are green
     * @throws BlockIOException
     */
    public GreenTransactions isGreenTransaction(String[] txIDs) throws BlockIOException {
        if (txIDs.length == 0) {
            throw new IllegalArgumentException("You have to provide at least one transaction ID.");
        }

        String paramString = txIDs.length == 1 ? txIDs[0] : Arrays.asList(txIDs).toString().replaceAll("^\\[|\\]$", "");
        HashMap<String, String> params = new HashMap<String, String>(1);
        params.put(Constants.Params.TX_IDS, paramString);

        Response.ResponseGreenTransactions response = (Response.ResponseGreenTransactions) doApiCall(Constants.Methods.IS_GREEN_TX, params, Response.ResponseGreenTransactions.class);
        return response.greenTransactions;
    }

    /**
     * Lists up to 100 of the last transactions received by the account associated with this clients' API key
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsReceived} object containing the list of received transactions
     * @throws BlockIOException
     */
    public TransactionsReceived getTransactionsReceived(String beforeTX) throws BlockIOException {
        return (TransactionsReceived) abstractTransactionRequest(null, null, beforeTX, Constants.Values.TYPE_RECEIVED);
    }

    /**
     * Lists up to 100 of the last transactions received by the provided addresses of the account associated with this clients' API key
     * @param addresses A String array containing the addresses to request transactions for
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsReceived} object containing the list of received transactions
     * @throws BlockIOException
     */
    public TransactionsReceived getTransactionsReceivedByAddress(String[] addresses, String beforeTX) throws BlockIOException {
        return (TransactionsReceived) abstractTransactionRequest(addresses, Constants.Params.ADDRS, beforeTX, Constants.Values.TYPE_RECEIVED);
    }

    /**
     * Lists up to 100 of the last transactions received by the provided labels of the account associated with this clients' API key
     * @param labels A String array containing the labels to request transactions for
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsReceived} object containing the list of received transactions
     * @throws BlockIOException
     */
    public TransactionsReceived getTransactionsReceivedByLabel(String[] labels, String beforeTX) throws BlockIOException {
        return (TransactionsReceived) abstractTransactionRequest(labels, Constants.Params.LABELS, beforeTX, Constants.Values.TYPE_RECEIVED);
    }

    /**
     * Lists up to 100 of the last transactions received by the provided user IDs of the account associated with this clients' API key
     * @param userIDs A String array containing the user IDs to request transactions for
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsReceived} object containing the list of received transactions
     * @throws BlockIOException
     */
    public TransactionsReceived getTransactionsReceivedByUserID(String[] userIDs, String beforeTX) throws BlockIOException {
        return (TransactionsReceived) abstractTransactionRequest(userIDs, Constants.Params.USER_IDS, beforeTX, Constants.Values.TYPE_RECEIVED);
    }

    /**
     * Lists up to 100 of the last transactions sent by the account associated with this clients' API key
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsSent} object containing the list of sent transactions
     * @throws BlockIOException
     */
    public TransactionsSent getTransactionsSent(String beforeTX) throws BlockIOException {
        return (TransactionsSent) abstractTransactionRequest(null, null, beforeTX, Constants.Values.TYPE_SENT);
    }

    /**
     * Lists up to 100 of the last transactions sent by the provided addresses of the account associated with this clients' API key
     * @param addresses A String array containing the addresses to request transactions for
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsSent} object containing the list of sent transactions
     * @throws BlockIOException
     */
    public TransactionsSent getTransactionsSentByAddress(String[] addresses, String beforeTX) throws BlockIOException {
        return (TransactionsSent) abstractTransactionRequest(addresses, Constants.Params.ADDRS, beforeTX, Constants.Values.TYPE_SENT);
    }

    /**
     * Lists up to 100 of the last transactions sent by the provided labels of the account associated with this clients' API key
     * @param labels A String array containing the labels to request transactions for
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsSent} object containing the list of sent transactions
     * @throws BlockIOException
     */
    public TransactionsSent getTransactionsSentByLabel(String[] labels, String beforeTX) throws BlockIOException {
        return (TransactionsSent) abstractTransactionRequest(labels, Constants.Params.LABELS, beforeTX, Constants.Values.TYPE_SENT);
    }

    /**
     * Lists up to 100 of the last transactions sent by the provided user IDs of the account associated with this clients' API key
     * @param userIDs A String array containing the user IDs to request transactions for
     * @param beforeTX An optional transaction ID used as upper bound of the requested transactions. Use this to request more than 100 of the last transactions
     * @return A {@link TransactionsSent} object containing the list of sent transactions
     * @throws BlockIOException
     */
    public TransactionsSent getTransactionsSentByUserID(String[] userIDs, String beforeTX) throws BlockIOException {
        return (TransactionsSent) abstractTransactionRequest(userIDs, Constants.Params.USER_IDS, beforeTX, Constants.Values.TYPE_SENT);
    }

    private Object abstractTransactionRequest(String[] whatFor, String typeOfParams, String beforeTx, String type) throws BlockIOException {
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(Constants.Params.TYPE, type);

        if (whatFor != null && typeOfParams != null) {
            if (whatFor.length == 0) {
                throw new IllegalArgumentException("You have to provide at least one address/label/user ID.");
            }
            String paramString = whatFor.length == 1 ? whatFor[0] : Arrays.asList(whatFor).toString().replaceAll("^\\[|\\]$", "");
            params.put(typeOfParams, paramString);
        }

        if (beforeTx != null && !beforeTx.equals("")) {
            params.put(Constants.Params.BEFORE_TX, beforeTx);
        }

        if (type.equals(Constants.Values.TYPE_RECEIVED)) {
            Response.ResponseTransactionsReceived response = (Response.ResponseTransactionsReceived) doApiCall(Constants.Methods.GET_TXNS, params, Response.ResponseTransactionsReceived.class);
            return response.transactionsReceived;
        } else if (type.equals(Constants.Values.TYPE_SENT)) {
            Response.ResponseTransactionsSent response = (Response.ResponseTransactionsSent) doApiCall(Constants.Methods.GET_TXNS, params, Response.ResponseTransactionsSent.class);
            return response.transactionsSent;
        } else {
            throw new IllegalArgumentException("Internal error. Please file an issue report");
        }
    }

    private Response doApiCall(String method, Map<String, String> params, Class<?> responseType) throws BlockIOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet request = new HttpGet(Constants.buildUri(method));
        URIBuilder uriBuilder = new URIBuilder(request.getURI());
        uriBuilder.addParameter(Constants.Params.API_KEY, apiKey);

        if (params != null) {
            for (Map.Entry<String, String> entry: params.entrySet()) {
                uriBuilder.addParameter(entry.getKey(), entry.getValue());
            }
        }

        CloseableHttpResponse response;
        try {
            request.setURI(uriBuilder.build());
            response = client.execute(request);
            return getResponse(response, responseType);
        } catch (IOException e) {
            throw new BlockIOException("Network connectivity problem.");
        } catch (URISyntaxException e) {
            throw new BlockIOException("URI build failed. That is an internal error. Please file an issue.");
        }
    }

    private Response doPostApiCall(String method, Map<String, String> params, Class<?> responseType) throws BlockIOException {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost request = new HttpPost(Constants.buildUri(method, true));
        List<NameValuePair> postParams = new ArrayList<NameValuePair>(2);

        postParams.add(new BasicNameValuePair(Constants.Params.API_KEY, apiKey));

        if (params != null) {
            for (Map.Entry<String, String> entry: params.entrySet()) {
                postParams.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
        }
        postParams.add(new BasicNameValuePair("priority", "low"));

        CloseableHttpResponse response;
        try {
            request.setEntity(new UrlEncodedFormEntity(postParams));
            response = client.execute(request);
            return getResponse(response, responseType);
        } catch (IOException e) {
            throw new BlockIOException("Network connectivity problem.");
        }
    }

    private Response getResponse(CloseableHttpResponse response, Class<?> responseType) throws BlockIOException {
        Gson gson = new Gson();
        String responseString;
        try {
            responseString = EntityUtils.toString(response.getEntity());
            response.close();
        } catch (IOException e) {
            throw new BlockIOException("Received invalid data from API.");
        }

        switch (response.getStatusLine().getStatusCode()) {
            case HttpStatus.SC_OK:
                return (Response) gson.fromJson(responseString, responseType);
            case HttpStatus.SC_NOT_FOUND:
                Response.ResponseError error = gson.fromJson(responseString, Response.ResponseError.class);
                throw new BlockIOException("API returned error: " + error.error.message);
            default:
                throw new BlockIOException("Unknown API response.");
        }
    }
}
