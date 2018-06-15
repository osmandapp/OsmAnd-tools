package net.osmand.bitcoinsender.utils;

public class Constants {

    public static final String BASE_URL = "https://block.io/api/v";
    public static final String API_VERSION = "2";

    public static class Methods {
        public static final String GET_NEW_ADDRESS = "get_new_address";
        public static final String GET_ACCOUNT_BALANCE = "get_balance";
        public static final String GET_MY_ADDRESSES = "get_my_addresses";
        public static final String GET_ADDR_BALANCE = "get_address_balance";
        public static final String GET_ADDR_BY_LABEL = "get_address_by_label";

        public static final String WITHDRAW_FROM_ANY = "withdraw";
        public static final String WITHDRAW_FROM_LABELS = "withdraw_from_labels";
        public static final String WITHDRAW_FROM_ADDRS = "withdraw_from_addresses";
        public static final String WITHDRAW_FROM_USERIDS = "withdraw_from_user_ids";
        public static final String WITHDRAW_DO_FINAL = "sign_and_finalize_withdrawal";

        public static final String GET_PRICES = "get_current_price";
        public static final String IS_GREEN_ADDR = "is_green_address";
        public static final String IS_GREEN_TX = "is_green_transaction";
        public static final String GET_TXNS = "get_transactions";

        public static final String GET_FEE = "get_network_fee_estimate";

    }

    public static class Params {
        public static final String API_KEY = "api_key";
        public static final String LABEL = "label";
        public static final String LABELS = "labels";
        public static final String ADDRS = "addresses";
        public static final String FROM_LABELS = "from_labels";
        public static final String TO_LABELS = "to_labels";
        public static final String FROM_ADDRS = "from_addresses";
        public static final String TO_ADDRS = "to_addresses";
        public static final String FROM_USERIDS = "from_user_ids";
        public static final String TO_USERIDS = "to_user_ids";
        public static final String AMOUNTS = "amounts";
        public static final String PIN = "pin";
        public static final String SIG_DATA = "signature_data";

        public static final String PRICE_BASE = "price_base";
        public static final String TX_IDS = "transaction_ids";
        public static final String TYPE = "type";
        public static final String BEFORE_TX = "before_tx";
        public static final String USER_IDS = "user_ids";

    }

    public static class Values {
        public static final String TYPE_SENT = "sent";
        public static final String TYPE_RECEIVED = "received";
    }

    public static String buildUri(String method) {
        return buildUri(method, false);
    }

    public static String buildUri(String method, boolean removeTrailingSlash) {
        String s = BASE_URL + API_VERSION + "/" + method;
        return removeTrailingSlash ? s : s + "/";
    }

}
