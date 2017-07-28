package net.osmand.bitcoinsender;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;
import net.osmand.bitcoinsender.model.AccountBalance;
import net.osmand.bitcoinsender.model.Withdrawal;
import net.osmand.bitcoinsender.utils.BlockIOException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by Paul on 07.06.17.
 */
public class CoinSenderMain {

    private static String guid;
    private static String pass;
    private static String directory;

    public static void main(String args[]) throws IOException {

        if (args.length <= 0) {
            System.out.println("Usage: --prepare | --send");
            return;
        }

        System.out.print("Enter your API key: ");
        Scanner in = new Scanner(System.in);
        guid = in.nextLine();
        System.out.print("Enter your PIN: ");
        pass = in.nextLine();
        System.out.print("Enter full path to JSON file: ");
        directory = in.nextLine();

        if (guid.equals("") || pass.equals("")) {
            System.out.println("You forgot to enter Client_ID or Secret. Exiting...");
            return;
        }

        BlockIO api = new BlockIO(guid);
        Map<String, Object> recipients = new LinkedHashMap<String, Object>();

        System.out.println();

        AccountBalance balance = null;
        try {
            balance = api.getAccountBalance();
        } catch (BlockIOException e) {
            e.printStackTrace();
            System.out.println("Incorrect API key. Exiting...");
            return;
        }
        System.out.println("Balance for account " + balance.network
                + ": Confirmed: " + balance.availableBalance
                + " Pendning: " + balance.getPendingReceivedBalance());
        System.out.println();


        Gson gson = new Gson();
        File file = new File(directory);
        if (!file.exists()) {
            while (!file.exists()) {
                System.out.print("You have entered incorrect file path. Please try again: ");
                directory = in.nextLine();
                file = new File(directory);
            }
        }
        JsonReader reader = new JsonReader(new FileReader(directory));
        recipients.putAll((Map) gson.fromJson(reader, Map.class));
        List<LinkedTreeMap> paymentsList = (ArrayList) recipients.get("payments");
        Map<String, Double> payments = new LinkedHashMap<String, Double>();
        double allMoney = 0;
        for (LinkedTreeMap map : paymentsList) {
            Double sum = (Double) map.get("btc");
            allMoney += sum;
            if (payments.containsKey(map.get("btcaddress"))) {
                sum += payments.get(map.get("btcaddress"));
            }
            payments.put((String) map.get("btcaddress"), sum);
        }
        List<Map> splitPayment = splitResults(payments);
        for (Map<String, Double> map : splitPayment) {
            payments.putAll(map);
        }

        if (args.length > 0) {
            if (args[0].equals("--prepare")) {
                String totalSum = String.format("%.12f", (allMoney));
                System.out.println("Number of chunks: " + splitPayment.size());
                System.out.println("Total sum in BTC: " + totalSum);
            }
            else if (args[0].equals("--send")) {
                boolean done = false;
                List<Integer> paidChunks = new ArrayList<Integer>();
                while (!done) {
                    System.out.println("Number of chunks: " + splitPayment.size());
                    System.out.println("You have already paid for these chunks: " + paidChunks.toString());
                    System.out.print("Enter the number of chunk you want to pay for ('-1' to exit): ");
                    int chunk = in.nextInt() - 1;
                    if (chunk == -2) {
                        break;
                    }
                    while (chunk < 0 || chunk >= splitPayment.size()) {
                        System.out.print("Please enter a number between 1 and " + splitPayment.size() + ": ");
                        chunk = in.nextInt() - 1;
                        if (chunk == -2) {
                            break;
                        }
                    }
                    if (paidChunks.contains(chunk + 1)) {
                        System.out.println("You've already paid for this chunk!");
                        continue;
                    }
                    Map<String, Double> currentPayment = splitPayment.get(chunk);
                    System.out.println("All payments: ");
                    double total = 0l;
                    for (String key : currentPayment.keySet()) {
                        total += currentPayment.get(key);
                        String currentPaymentString = String.format("%.12f", currentPayment.get(key));
                        System.out.println("Address: " + key + ", BTC: " + currentPaymentString);
                    }
                    Scanner scanner = new Scanner(System.in);
                    String totalString = String.format("%.12f", total);
                    System.out.println("Total: " + totalString);
                    System.out.println();
                    api.printFeeForTransaction(currentPayment);
                    System.out.println();
                    System.out.print("Are you sure you want to pay " + totalString + " BTC? [y/n]: ");
                    String answer = scanner.nextLine();

                    if (!answer.toLowerCase().equals("y")) {
                        continue;
                    }
                    int chunkUI = (chunk + 1);
                    System.out.println("Paying for chunk " + chunkUI + "...");

                    Withdrawal withdrawal = null;
                    try {
                        withdrawal = api.withdraw(null, null, currentPayment, BlockIO.ParamType.ADDRS, pass);
                        paidChunks.add(chunkUI);
                    } catch (BlockIOException e) {
                        e.printStackTrace();
                        System.out.println("Unable to pay fo this chunk");
                        continue;
                    }

                    System.out.println("Withdrawal done. Transaction ID: " + withdrawal.txid
                            + " Amount withdrawn: " + withdrawal.amountWithdrawn
                            + " Amount sent: " + withdrawal.amountSent
                            + " Network fee: " + withdrawal.networkFee
                            + " Block.io fee: " + withdrawal.blockIOFee);

                    if (splitPayment.size() == paidChunks.size()) {
                        System.out.println("You've successfully paid for all chunks!");
                        System.out.println("Exiting...");
                        break;
                    }
                }
            }

        }


    }

    private static List splitResults(Map<String, Double> map) {
        List<Map<String, Double>> res = new ArrayList<Map<String, Double>>();
        Map<String, Double> part = new LinkedHashMap<String, Double>();
        int count = 1;
        for (String key : map.keySet()) {
            part.put(key, map.get(key));
            if (part.size() == 50 || count == map.keySet().size()) {
                res.add(part);
                part = new LinkedHashMap<String, Double>();
            }
            count++;
        }

        res.remove(8);
        res.remove(7);
        res.remove(3);
        res.remove(1);
        res.remove(0);

        return res;
    }
}
