package net.osmand.bitcoinsender;

import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.util.*;

public class TransactionAnalyzer {

    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length < 3 ) {
            System.out.println("Usage: <path_to_json_report> <path_to_folder_with_montly_transactions> <path_to_result_json>");
            System.exit(-1);
        }
        List<LinkedTreeMap> paymentsParsed = CoinSenderMain.getPayments(new FileReader(args[0]));
        Map<String, Double> payments = CoinSenderMain.convertPaymentsToMap(paymentsParsed);
        payments = filterPayments(payments, 0.0001d);
        Map<String, Double> payouts = getPayoutsForMonth(args[1]);
        calculateDebt(payments, payouts, args[2]);
    }

    private static Map<String,Double> filterPayments(Map<String, Double> payments, double filter) {
        Map<String,Double> res = new HashMap<>();
        for (String key : payments.keySet()) {
            if (payments.get(key) < filter) {
                res.put(key, payments.get(key));
            }
        }
        return res;
    }

    private static Map<String, Double> getPayoutsForMonth(String directory) throws FileNotFoundException {
        File dir = new File(directory);
        if (!dir.isDirectory()) {
            System.out.println(directory + " is not a directory");
            System.exit(-1);
        }
        Map<String, Double> result = new HashMap<>();

        for (File f : dir.listFiles()) {
            Map<String, Object> payoutObjects = new LinkedHashMap<String, Object>();
            JsonReader reader = new JsonReader(new FileReader(f));
            payoutObjects.putAll((Map) gson.fromJson(reader, Map.class));
            LinkedTreeMap data = (LinkedTreeMap) payoutObjects.get("data");
            List<LinkedTreeMap> outputs = (ArrayList) data.get("outputs");
            for (LinkedTreeMap payout : outputs) {
                String address = (String) payout.get("address");
                Double sum = Double.valueOf((String) payout.get("value"));
                if (result.containsKey(address)) {
                    result.put(address, result.get(address) + sum);
                } else {
                    result.put(address, sum);
                }
            }

        }
        return result;
    }

    private static void calculateDebt(Map<String, Double> payments, Map<String, Double> payouts, String pathname) {
        for (String key : payments.keySet()) {
            if (payouts.containsKey(key)) {
                if (payouts.get(key).equals(payments.get(key))) {
                    payments.remove(key);
                }
            }
        }
        File f = new File(pathname);
        BufferedWriter writer = null;
        try
        {
            JsonArray ja = new JsonArray();
            for (String key : payments.keySet()) {
                JsonObject el = new JsonObject();
                el.addProperty(key, payments.get(key));
                ja.add(el);
            }
            JsonObject jo = new JsonObject();
            jo.add("not_payed", ja);
            writer = new BufferedWriter( new FileWriter(f));
            writer.write(jo.toString());
            writer.close();

        }
        catch ( IOException e)
        {
            e.printStackTrace();
        }
    }

}
