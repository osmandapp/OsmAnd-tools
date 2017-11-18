package net.osmand.bitcoinsender;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;

public class TransactionAnalyzer {

    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static final int BEGIN_YEAR = 2016;
    private static final int UNDERPAYED_THRESHOLD = 10;
    private static final int OVERPAYED_THRESHOLD = 1000;
	private static final String REPORT_URL = "http://builder.osmand.net/reports/query_month_report.php?report=getPayouts&month=";
	private static final String TRANSACTIONS = "https://raw.githubusercontent.com/osmandapp/osmandapp.github.io/master/website/reports/transactions.json";
    public static void main(String[] args) throws IOException {
    	int FINAL_YEAR = Calendar.getInstance().get(Calendar.YEAR);
    	int FINAL_MONTH = Calendar.getInstance().get(Calendar.MONTH) ;
    	if(FINAL_MONTH == 0) {
    		FINAL_MONTH = 12;
    		FINAL_YEAR--;
    	}
    	System.out.println("Read all transcation ids...");
    	JsonReader tx = readJsonUrl(TRANSACTIONS, "", "transactions.json");
    	Map<String, Double> payedOut = calculatePayouts(tx);
    	// transactions url 
    	Map<String, Double> toPay = new HashMap<>();
    	for(int year = BEGIN_YEAR; year <= FINAL_YEAR; year++) {
    		int fMonth = year == FINAL_YEAR ? FINAL_MONTH : 12;
			for (int month = 1; month <= fMonth; month++) {
				String period = year + "-";
				if (month < 10) {
					period += "0";
				}
				period += month;
				System.out.println("Processing " + period + "... ");
				Map<?, ?> payoutObjects = gson.fromJson(readJsonUrl(REPORT_URL, period, "payout_"), Map.class);
				List<Map<?, ?>> outputs = (List<Map<?, ?>>) payoutObjects.get("payments");
				for (Map<?, ?> payout : outputs) {
					String address = (String) payout.get("btcaddress");
					Double sum = ((Double) payout.get("btc")) * 1000 * 1000 * 100;
					if (toPay.containsKey(address)) {
						toPay.put(address, toPay.get(address) + sum);
					} else {
						toPay.put(address, sum);
					}
				}
				
			}
    	}
    	for(String addrToPay : toPay.keySet()) {
    		Double sumToPay = toPay.get(addrToPay);
    		Double paid = payedOut.get(addrToPay);
    		if(paid == null) {
    			paid = 0d;
    		}
    		if(sumToPay < paid - OVERPAYED_THRESHOLD) {
    			System.out.println("??? " + addrToPay + " "+ (sumToPay - paid) + " satoshis");
    		} else if(paid < sumToPay - UNDERPAYED_THRESHOLD ) {
    			System.out.println("!!! " + addrToPay + " " + (sumToPay - paid) + " satoshis");
    		}
    	}
    	System.out.println(toPay.toString());
    	
    	
    }

	@SuppressWarnings("unchecked")
	private static Map<String, Double> calculatePayouts(JsonReader tx) throws JsonIOException, JsonSyntaxException, IOException {
		Gson gson = new Gson();
		Map<?, ?> mp = gson.fromJson(tx, Map.class);
		System.out.println(mp);
		Map<String, Double> result = new HashMap<>();
		for(Map.Entry<?, ?> e: mp.entrySet()) {
			System.out.println("Read transactions for " + e.getKey() +"... ");
			List<?> array = (List<?>)((Map<?, ?>)e.getValue()).get("transactions");
			for (Object tid : array) {
				String turl = "https://blockchain.info/rawtx/" + tid;
				System.out.println("Read transactions for " + turl + "... ");
				Map<?, ?> payoutObjects = gson.fromJson(readJsonUrl("https://blockchain.info/rawtx/", tid.toString(), "btc_"), Map.class);
//				Map<?, ?> data = (Map<?, ?>) payoutObjects.get("data");
				List<Map<?, ?>> outputs = (List<Map<?, ?>>) payoutObjects.get("out");
				for (Map<?, ?> payout : outputs) {
					String address = (String) payout.get("addr");
					Double sum = (Double) payout.get("value");
					if (result.containsKey(address)) {
						result.put(address, result.get(address) + sum);
					} else {
						result.put(address, sum);
					}
				}
			}
		}
		System.out.println(result);
		return result;
	}

	private static JsonReader readJsonUrl(String urlBase, String id, String cachePrefix) throws IOException {
		File fl = new File("cache/"+cachePrefix + id);
		if(fl.exists()) {
			return new JsonReader(new FileReader(fl));
		}
		URL url = new URL(urlBase + id);
		InputStream is = url.openStream();
		ByteArrayOutputStream bous = new ByteArrayOutputStream();
		byte[] bs = new byte[1024];
		int l;
		while ((l = is.read(bs)) != -1) {
			bous.write(bs, 0, l);
		}
		is.close();
		FileOutputStream fous = new FileOutputStream(fl);
		fous.write(bous.toByteArray());
		fous.close();
		JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(bous.toByteArray())));
		return reader;
	}


    

}
