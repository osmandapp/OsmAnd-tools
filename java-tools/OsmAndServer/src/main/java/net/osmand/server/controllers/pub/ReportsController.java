package net.osmand.server.controllers.pub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import net.osmand.bitcoinsender.TransactionAnalyzer;
import net.osmand.data.changeset.OsmAndLiveReportType;
import net.osmand.data.changeset.OsmAndLiveReports;
import net.osmand.data.changeset.OsmAndLiveReports.RecipientsReport;
import net.osmand.util.Algorithms;

@RestController
@RequestMapping("/reports")
public class ReportsController {
    protected static final Log LOGGER = LogFactory.getLog(ReportsController.class);
    public static final String REPORTS_FOLDER = "reports";
    public static final String TRANSACTIONS_FILE = REPORTS_FOLDER + "/transactions.json";
    private static final String REPORT_URL = "https://osmand.net/reports/query_month_report?report=getPayouts&month=";
    private static final String TXS_CACHE = REPORTS_FOLDER + "/txs/btc_";
    private static final String PAYOUTS_CACHE_ID = REPORTS_FOLDER + "/payouts/payout_";
    private static final String OSMAND_BTC_DONATION_ADDR = "1GRgEnKujorJJ9VBa76g8cp3sfoWtQqSs4";
    
    private static final String FEE_ESTIMATED_MODE = "ECONOMICAL";
    private static int TARGET_NUMBER_OF_BLOCKS = 50;
    public static final int SOCKET_TIMEOUT = 15 * 1000;

    private static final int BEGIN_YEAR = 2016;
    public static final long BITCOIN_SATOSHI = 1000 * 1000 * 100;
	public static final int MBTC_SATOSHI = 100 * 1000;
    
	// MIN PAY FORMULA
	public static final int AVG_TX_SIZE = 50; // 50 bytes
	public static final double FEE_PERCENT = 0.05; // fee shouldn't exceed 5%
	public static int FEE_BYTE_SATOSHI = 5; // varies over time in Bitcoin
	
	private final String btcJsonRpcUser;
	private final String btcJsonRpcPwd;
	
    @Value("${web.location}")
    private String websiteLocation;
    
    @Value("${gen.location}")
    private String genLocation;
    
    @Autowired
    private DataSource dataSource;
    
    private BtcTransactionReport btcTransactionReport = new BtcTransactionReport();
    
	private CloseableHttpClient httpclient;
	private RequestConfig requestConfig;
	private Gson formatter;
    
    
    private ReportsController() {
    	btcJsonRpcUser = System.getenv("BTC_JSON_RPC_USER");
    	btcJsonRpcPwd = System.getenv("BTC_JSON_RPC_PWD");
    	formatter = new Gson();
		if (btcJsonRpcUser != null) {
			httpclient = HttpClientBuilder.create().setSSLHostnameVerifier(new NoopHostnameVerifier())
					.setConnectionTimeToLive(SOCKET_TIMEOUT, TimeUnit.MILLISECONDS).setMaxConnTotal(20).build();
			requestConfig = RequestConfig.copy(RequestConfig.custom().build()).setSocketTimeout(SOCKET_TIMEOUT)
					.setConnectTimeout(SOCKET_TIMEOUT).setConnectionRequestTimeout(SOCKET_TIMEOUT).build();
		}
    }
    
    public static class AddrToPay {
    	public long totalPaid;
    	public long toPay;
    	public long totalToPay;
    	public String osmId = "";
		public String btcAddress;
    }
    
    public static class BtcToPayBalance {
    	public Map<String, Long> totalToPay = new TreeMap<>();
    	public Map<String, String> osmid = new HashMap<>();
    	public List<AddrToPay> toPay = new ArrayList<AddrToPay>();
    	public int defaultFee ;
    	public long minToPayoutSat;
    	
    	public long date;
    	public String generatedDate;
    	
    	public BtcToPayBalance () {
    		defaultFee = FEE_BYTE_SATOSHI;
    		minToPayoutSat = getMinSatoshiPay();
    		
    	}
    	
    	public long payWithFeeSat;
		public int payWithFeeCnt;
		
		public long payNoFeeSat;
		public int payNoFeeCnt;
		
		public int overpaidCnt;
		public long overpaidSat;
		
		public int overpaidFeeCnt;
		public long overpaidFeeSat;
		
    }
    
    public static class BtcTransactionReport {
    	// Payouts 
    	public Map<String, BtcTransactionsMonth> mapTransactions = new TreeMap<>();
    	public List<BtcTransactionsMonth> txs = new ArrayList<>();
		public long total;
		public Map<String, Long> totalPayouts = new HashMap<>();
		
		// To be paid
		public BtcToPayBalance balance = new BtcToPayBalance();
		
		// Current local balance
		public long walletBalance;
		public Map<String,Float> walletAddresses = new TreeMap<>();
		public int walletWaitingBlocks;
		public long walletTxFee;
		public int walletEstBlocks;
		public long walletEstFee;
		public String walletLasttx;
		
		
    }
    
    public static class BtcTransaction {
    	public String id;
    	public long total;
    	public long fee;
    	public int size = 1;
    	public int blockIndex = -1;
		public String url;
		public String rawurl;
    }
    
    public static class BtcTransactionsMonth {
    	public List<String> transactions = new ArrayList<String>();
    	public List<BtcTransaction> txValues = new ArrayList<BtcTransaction>();
		public String month;
		public long total;
		public long fee;
		public Map<String, Long> totalPayouts;
		public int size = 1;
    }
    
    @SuppressWarnings("unchecked")
	public BtcTransactionReport generateBtcReport(boolean genBalanceReport) {
		File transactions = new File(websiteLocation, TRANSACTIONS_FILE);
		if(transactions.exists()) {
			try {
				BtcTransactionReport rep = new BtcTransactionReport();
				Type tp = new TypeToken<Map<String, BtcTransactionsMonth> >() {}.getType();
				rep.mapTransactions = (Map<String, BtcTransactionsMonth>) formatter.fromJson(new FileReader(transactions),tp);
				for(Map.Entry<String, BtcTransactionsMonth> key : rep.mapTransactions.entrySet()) {
					BtcTransactionsMonth t = key.getValue();
					t.month = key.getKey();
					rep.txs.add(t);
				}
				for (BtcTransactionsMonth t : rep.txs) {
					loadPayouts(t);
					rep.total += t.total;
					for(String addr: t.totalPayouts.keySet()) {
						long paid = t.totalPayouts.get(addr);
						Long pd = rep.totalPayouts.get(addr);
						rep.totalPayouts.put(addr, (pd == null ? 0 : pd.longValue()) + paid);
					}
				}
				
				if (btcJsonRpcUser != null) {
					generateWalletStatus(rep);
					if(genBalanceReport) {
						try {
							btcRpcCall("settxfee", ((double)FEE_BYTE_SATOSHI ) / MBTC_SATOSHI);
						} catch (Exception e) {
							LOGGER.error("Error to set fee: " + e.getMessage(), e);
						}
					}
				}
				if(genBalanceReport) {
					rep.balance = generateBalanceToPay(rep);
				} else {
					rep.balance = btcTransactionReport.balance;
				}
				btcTransactionReport = rep;
			} catch (Exception e) {
				LOGGER.error("Fails to read transactions.json: " + e.getMessage(), e);
			}
		}
		
		return btcTransactionReport;
	}


	@SuppressWarnings("unchecked")
	private void generateWalletStatus(BtcTransactionReport rep) {
		try {
			List<Map<?, ?>> adrs = (List<Map<?, ?>>) btcRpcCall("listunspent");
			if (adrs != null) {
				for (Map<?, ?> addr : adrs) {
					String address = addr.get("address").toString();
					Float f = rep.walletAddresses.get(address);
					rep.walletAddresses.put(address, ((Number)addr.get("amount")).floatValue() + 
							(f == null ? 0 : f.floatValue()));
				}
			}
			Map<?, ?> winfo = (Map<?, ?>) btcRpcCall("getwalletinfo");
			if (winfo != null) {
				rep.walletTxFee = (long) (((Number) winfo.get("paytxfee")).doubleValue()
						* MBTC_SATOSHI);
				btcTransactionReport.walletBalance = (long) (((Number) winfo.get("balance")).doubleValue()
						* BITCOIN_SATOSHI);
			}
			rep.walletWaitingBlocks = TARGET_NUMBER_OF_BLOCKS;
			Map<?, ?> estFee = (Map<?, ?>) btcRpcCall("estimatesmartfee", TARGET_NUMBER_OF_BLOCKS, FEE_ESTIMATED_MODE);
			if (estFee != null) {
				rep.walletEstFee = (long) (((Number) estFee.get("feerate")).doubleValue() * MBTC_SATOSHI);
				rep.walletEstBlocks = (((Number) estFee.get("blocks")).intValue());
			}
			List<Map<?, ?>> lastTx = (List<Map<?, ?>>) btcRpcCall("listtransactions", "*", 1);
			if (lastTx != null) {
				rep.walletLasttx = (String) lastTx.get(0).get("txid");
			}
		} catch (Exception e) {
			LOGGER.error("Error to request balance: " + e.getMessage(), e);
		}
	}


	private Object btcRpcCall(String method, Object... pms) throws UnsupportedEncodingException, IOException, ClientProtocolException {
		HttpPost httppost = new HttpPost("http://" + btcJsonRpcUser + ":" + btcJsonRpcPwd + "@127.0.0.1:8332/");
		httppost.setConfig(requestConfig);
		httppost.addHeader("charset", StandardCharsets.UTF_8.name());

		Map<String, Object> params = new HashMap<>();
		params.put("jsonrpc", "1.0");
		params.put("id", "server");
		params.put("method", method);
		if(pms != null && pms.length > 0) {
			params.put("params", Arrays.asList(pms));
		}
		StringEntity entity = new StringEntity(formatter.toJson(params));
		LOGGER.info("Btc rpc send with content: " +  formatter.toJson(params));
		httppost.setEntity(entity);
		try (CloseableHttpResponse response = httpclient.execute(httppost)) {
			HttpEntity ht = response.getEntity();
			BufferedHttpEntity buf = new BufferedHttpEntity(ht);
			String result = EntityUtils.toString(buf, StandardCharsets.UTF_8);
			Map<?, ?> res = formatter.fromJson(new JsonReader(new StringReader(result)), Map.class);
			LOGGER.info("Result: " + result);
			if(res.get("result") != null) {
				return res.get("result");
			} else {
				return null;
			}
		}
	}
    
    public BtcTransactionReport getBitcoinTransactionReport() {
    	if(btcTransactionReport.mapTransactions.isEmpty()) {
    		generateBtcReport(false);
    	}
		return btcTransactionReport;
	}
    
    public void reloadConfigs(List<String> errors) {
    	generateBtcReport(false);
	}
    
    public String payOutBitcoin(BtcTransactionReport rep, int batchSize) throws IOException {
    	Map<String, String> toPay = new LinkedHashMap<String, String>();
    	for(int i = 0; i < batchSize && i < rep.balance.toPay.size(); i++) {
    		AddrToPay add = rep.balance.toPay.get(i);
    		if(add.btcAddress.equals(OSMAND_BTC_DONATION_ADDR)) {
    			continue;
    		}
    		toPay.put(add.btcAddress, ((double)add.toPay / BITCOIN_SATOSHI) + "");
    	}
    	String txId = (String) btcRpcCall("sendmany", 
    			"", // dummy default
    			toPay, // map to pay 
    			TARGET_NUMBER_OF_BLOCKS, // dummy wait confirmations 
    			"https://osmand.net/osm_live", // comment
    			new String[0], // subtractfeefrom - don't substract
    			true, // replaceable
    			TARGET_NUMBER_OF_BLOCKS, // conf_target
    			FEE_ESTIMATED_MODE // estimate_mode
    			);
    	return txId;
    }
    
    public void updateBitcoinReport(String defaultFee, String waitingBlocks) {
    	FEE_BYTE_SATOSHI = Algorithms.parseIntSilently(defaultFee, FEE_BYTE_SATOSHI) ;
    	TARGET_NUMBER_OF_BLOCKS = Algorithms.parseIntSilently(waitingBlocks, TARGET_NUMBER_OF_BLOCKS) ;
    	generateBtcReport(true);
    }
    
    
    @RequestMapping(path = { "/query_btc_balance_report"})
    @ResponseBody
	public String getBtcBalanceReport(HttpServletRequest request, HttpServletResponse response) throws SQLException, IOException {
    	BtcToPayBalance blnc = getBitcoinTransactionReport().balance;
    	if(blnc.totalToPay.isEmpty()) {
    		generateBtcReport(true);
    	}
    	return formatter.toJson(getBitcoinTransactionReport().balance);    	
    }
    
    @RequestMapping(path = { "/query_report", "/query_report.php", 
    		"/query_month_report", "/query_month_report.php"})
    @ResponseBody
	public String getReport(HttpServletRequest request, HttpServletResponse response, 
			@RequestParam(required = true) String report,
			@RequestParam(required = false) String month, @RequestParam(required = false) String region) throws SQLException, IOException {
    	Connection conn = DataSourceUtils.getConnection(dataSource);
    	
		try {
			if (request.getServletPath().contains("_month_")) {
				response.setHeader("Content-Description", "json report");
				response.setHeader("Content-Disposition", String.format("attachment; filename=%s%s%s.json", report,
						isEmpty(month) ? "" : ("-"+month), isEmpty(region) ? "" : ("-"+region)));
				response.setHeader("Content-Type", "application/json");
			}
			OsmAndLiveReports reports = new OsmAndLiveReports(conn, month);
			OsmAndLiveReportType type = null;
			switch (report) {
			case "all_countries":
				type = OsmAndLiveReportType.COUNTRIES;
				break;
			case "available_recipients":
			case "recipients_by_month":
				type = OsmAndLiveReportType.RECIPIENTS;
				break;
			case "ranking_users_by_month":
				type = OsmAndLiveReportType.USERS_RANKING;
				break;
			case "supporters_by_month":
				type = OsmAndLiveReportType.SUPPORTERS;
				break;
			case "total_changes_by_month":
				type = OsmAndLiveReportType.TOTAL_CHANGES;
				break;
			case "total":
				type = OsmAndLiveReportType.TOTAL;
				break;
			case "payouts":
			case "getPayouts":
				type = OsmAndLiveReportType.PAYOUTS;
				break;
			case "ranking":
				type = OsmAndLiveReportType.RANKING;
				break;
			default:
				break;
			}
			if (report.equals("all_contributions_requests")) {
				return String.format("[%s, %s, %s]", reports.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, region),
						reports.getJsonReport(OsmAndLiveReportType.RANKING, region),
						reports.getJsonReport(OsmAndLiveReportType.USERS_RANKING, region));
			}
			if(report.equals("recipients_by_month")) {
				Gson gson = reports.getJsonFormatter();
				RecipientsReport rec = reports.getReport(OsmAndLiveReportType.RECIPIENTS, region, RecipientsReport.class);
				BtcTransactionsMonth txs = generateBtcReport(false).mapTransactions.get(month);
				StringBuilder payouts = new StringBuilder(); 
				if(txs != null && txs != null) {
					if(txs.transactions.size() > 0) {
						int i = 1;
						payouts.append("Payouts:&nbsp;");
						for(String s : txs.transactions) {
							if( i > 1) {
								payouts.append(",&nbsp;");
							}
							payouts.append(
									String.format("<a href='https://blockchain.info/tx/%s'>Transaction #%d</a>", s, i++)); 
						}
					}
				}
				String worldCollectedMessage = String.format("<p>%.3f mBTC</p><span>total collected%s</span>",
						rec.btc * 1000, rec.notReadyToPay ? " (may change in the final report)" : "" );
				String regionCollectedMessage = String.format("<p>%.3f mBTC</p><span>collected for</span>",
						rec.regionBtc * 1000);
				rec.worldCollectedMessage = worldCollectedMessage;
				rec.regionCollectedMessage = regionCollectedMessage;
				rec.payouts = payouts.toString();
				StringBuilder reportBld = new StringBuilder();
				if(!rec.notReadyToPay) {
					reportBld.append(
							String.format("<a type='application/json' "
									+ "href='/reports/query_month_report?report=total&month=%1$s'  "
									+ "download='report-%1$s.json' >Download all json reports for %1$s</a>",month));
					reportBld.append(".&nbsp;&nbsp;");
					reportBld.append("<a type='application/json' "
							+ "href='https://builder.osmand.net/reports/report_underpaid.json.html'>"
							+ "Cumulative underpaid report</a>");
				}
				rec.reports =reportBld.toString();
				return gson.toJson(rec);
			}
			return reports.getJsonReport(type, region);
		} finally {
    		DataSourceUtils.releaseConnection(conn, dataSource);
    	}
	}

	private boolean isEmpty(String month) {
		return month == null || month.length() == 0;
	}
	
	@SuppressWarnings("unchecked")
	private void loadPayouts(BtcTransactionsMonth t) throws IOException {
		t.totalPayouts = new HashMap<>();
		t.total = 0;
		for (String tid : t.transactions) {
			BtcTransaction tx = new BtcTransaction();
			tx.id = tid;
			tx.rawurl = "https://blockchain.info/rawtx/" + tid;
			tx.url = "https://blockchain.info/tx/" + tid;
			String cacheId = TXS_CACHE + tid;
			tx.total = 0;
			t.txValues.add(tx);
			try {
				Map<?, ?> payoutObjects = formatter.fromJson(readJsonUrl(tx.rawurl, cacheId, true), Map.class);
				// Map<?, ?> data = (Map<?, ?>) payoutObjects.get("data");
				Map<String, String> ins = new TreeMap<String, String>();
				long totalIn = 0;
				long totalOut = 0;
				List<Map<?, ?>> inputs = (List<Map<?, ?>>) payoutObjects.get("inputs");
				for (Map<?, ?> inp: inputs) {
					Map<?, ?> in = (Map<?, ?>) inp.get("prev_out");
					String address = (String) in.get("addr");
					totalIn += ((Number) in.get("value")).longValue();
					ins.put(address, in.get("value").toString());
				}
				List<Map<?, ?>> outputs = (List<Map<?, ?>>) payoutObjects.get("out");
				for (Map<?, ?> payout : outputs) {
					String address = (String) payout.get("addr");
					long sum = ((Number) payout.get("value")).longValue();
					totalOut += sum;
					if(ins.containsKey(address)) {
						continue;
					}
					tx.total += sum;
					if (t.totalPayouts.containsKey(address)) {
						t.totalPayouts.put(address, t.totalPayouts.get(address) + sum);
					} else {
						t.totalPayouts.put(address, sum);
					}
				}
				tx.fee = totalIn - totalOut;
				tx.size = ((Number) payoutObjects.get("size")).intValue();
				if(payoutObjects.get("block_index") != null) { 
					tx.blockIndex = ((Number) payoutObjects.get("block_index")).intValue();
				}
				
				t.size += tx.size;
				t.fee += tx.fee;
				t.total += tx.total;
			} finally {
				if (tx.blockIndex <= 0) {
					getCacheFile(cacheId).delete();
				}
			}
		}
	}

	
	
	public static long getMinSatoshiPay() {
		// !!! PAYOUT * FEE_PERCENT >= AVG_TX_SIZE * FEE_BYTE_SATOSHI ;
		long minPayout = (long) (AVG_TX_SIZE * FEE_BYTE_SATOSHI / FEE_PERCENT);
		return minPayout;
	}
	
	protected BtcToPayBalance generateBalanceToPay(BtcTransactionReport report) throws IOException {
		BtcToPayBalance balance = new BtcToPayBalance();
		Date dt = new Date();
		balance.date = dt.getTime();
		balance.generatedDate = dt.toString();
    	collectAllNeededPayouts(balance);
    	for(String addrToPay : balance.totalToPay.keySet()) {
			AddrToPay a = new AddrToPay();
    		a.btcAddress = addrToPay;
    		a.totalToPay = balance.totalToPay.get(addrToPay);
    		Long paid = report.totalPayouts.get(addrToPay);
    		if(paid != null) {
    			a.totalPaid = paid.longValue();
    		}
    		a.toPay = a.totalToPay - a.totalPaid;
    		if(addrToPay.equals(OSMAND_BTC_DONATION_ADDR)) {
    			balance.toPay.add(a);
    			continue;
    		}
    		if(balance.osmid.containsKey(addrToPay)) {
    			a.osmId = balance.osmid.get(addrToPay);
    		}
    		if(a.toPay < 0) {
    			balance.overpaidCnt++;
    			balance.overpaidSat = balance.overpaidSat - a.toPay;
    			if(a.toPay <= -balance.minToPayoutSat) {
    				balance.overpaidFeeCnt++;
        			balance.overpaidFeeSat = balance.overpaidFeeSat - a.toPay;
        			balance.toPay.add(a);
    			}
    		} else if(a.toPay > 0) {
    			balance.payNoFeeCnt++;
    			balance.payNoFeeSat = balance.payNoFeeSat + a.toPay;
    			if(a.toPay >= balance.minToPayoutSat) {
    				balance.payWithFeeCnt++;
        			balance.payWithFeeSat = balance.payWithFeeSat + a.toPay;
    			}
    			balance.toPay.add(a);
    		}
    	}
    	for(String addrPaid : report.totalPayouts.keySet()) {
    		if(!balance.totalToPay.containsKey(addrPaid)) {
    			AddrToPay a = new AddrToPay();
        		a.btcAddress = addrPaid;
        		a.totalPaid = report.totalPayouts.get(addrPaid);
        		a.totalToPay = 0;
        		a.toPay = - a.totalPaid;
        		balance.toPay.add(a);
    		}
    	}
    	Collections.sort(balance.toPay, new Comparator<AddrToPay>() {

			@Override
			public int compare(AddrToPay o1, AddrToPay o2) {
				return -Long.compare(o1.toPay, o2.toPay);
			}
		});
		return balance;
	}

	@SuppressWarnings("unchecked")
	private void collectAllNeededPayouts(BtcToPayBalance toBePaid) throws IOException {
		int FINAL_YEAR = Calendar.getInstance().get(Calendar.YEAR);
		int FINAL_MONTH = Calendar.getInstance().get(Calendar.MONTH);
		if (FINAL_MONTH == 0) {
			FINAL_MONTH = 12;
			FINAL_YEAR--;
		}
		for (int year = BEGIN_YEAR; year <= FINAL_YEAR; year++) {
			int fMonth = year == FINAL_YEAR ? FINAL_MONTH : 12;
			for (int month = 1; month <= fMonth; month++) {
				String period = year + "-";
				if (month < 10) {
					period += "0";
				}
				period += month;
				Map<?, ?> payoutObjects = formatter
						.fromJson(readJsonUrl(REPORT_URL + period, PAYOUTS_CACHE_ID + month, false), Map.class);
				if (payoutObjects == null) {
					continue;
				}
				List<Map<?, ?>> outputs = (List<Map<?, ?>>) payoutObjects.get("payments");
				for (Map<?, ?> payout : outputs) {
					String inputAddress = (String) payout.get("btcaddress");
					String address = TransactionAnalyzer.simplifyBTC(inputAddress);
					if (address == null) {
						address = inputAddress;
					}
					String osmId = payout.get("osmid").toString();
					long sum = (long) (((Double) payout.get("btc")) * BITCOIN_SATOSHI);
					toBePaid.osmid.put(address, osmId);
					if (toBePaid.totalToPay.containsKey(address)) {
						toBePaid.totalToPay.put(address, toBePaid.totalToPay.get(address) + sum);
					} else {
						toBePaid.totalToPay.put(address, sum);
					}
				}

			}
		}
	}
	
	
	private JsonReader readJsonUrl(String rurl, String id, boolean cache)
			throws IOException {
		File fl = getCacheFile(id);
		fl.getParentFile().mkdirs();
		if (fl.exists() && cache) {
			return new JsonReader(new FileReader(fl));
		}
		URL url = new URL(rurl);
		InputStream is = url.openStream();
		ByteArrayOutputStream bous = new ByteArrayOutputStream();
		byte[] bs = new byte[1024];
		int l;
		while ((l = is.read(bs)) != -1) {
			bous.write(bs, 0, l);
		}
		is.close();
		if (cache) {
			FileOutputStream fous = new FileOutputStream(fl);
			fous.write(bous.toByteArray());
			fous.close();
		}
		JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(bous.toByteArray())));
		return reader;
	}

	private File getCacheFile(String id) {
		return new File(websiteLocation, id);
	}



}
