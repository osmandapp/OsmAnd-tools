package net.osmand.server.controllers.pub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import net.osmand.data.changeset.OsmAndLiveReportType;
import net.osmand.data.changeset.OsmAndLiveReports;
import net.osmand.data.changeset.OsmAndLiveReports.RecipientsReport;

@RestController
@RequestMapping("/reports")
public class ReportsController {
    protected static final Log LOGGER = LogFactory.getLog(ReportsController.class);
    public static final String REPORTS_FOLDER = "reports";
    public static final String TXS_FOLDER = REPORTS_FOLDER + "txs";
    public static final String TRANSACTIONS_FILE = REPORTS_FOLDER + "/transactions.json";

    public static final long BITCOIN_SATOSHI = 1000 * 1000 * 100;
	public static final int MBTC_SATOSHI = 100 * 1000;
    
    @Value("${web.location}")
    private String websiteLocation;
    
    @Value("${gen.location}")
    private String genLocation;
    
    @Autowired
    private DataSource dataSource;
    
    private BtcTransactionReport btcTransactionReport = new BtcTransactionReport();
    
    public static class BtcTransactionReport {
    	public Map<String, BtcTransactionsMonth> mapTransactions = new TreeMap<>();
    	public List<BtcTransactionsMonth> txs = new ArrayList<>();
		public long total;
		

		public long balance;
		public int defaultFee;
		public long payWithFeeSat;
		public int payWithFeeCnt;
		public long payNoFeeSat;
		public int payNoFeeCnt;
		public int overpaidCnt;
		public long overpaidSat;
    }
    
    public static class BtcTransaction {
    	public String id;
    	public long total;
    	public long fee;
    	public int blockIndex = -1;
		public String url;
    }
    
    public static class BtcTransactionsMonth {
    	public List<String> transactions = new ArrayList<String>();
    	public List<BtcTransaction> txValues = new ArrayList<BtcTransaction>();
		public String month;
		public long total;
		public long fee;
		public Map<String, Long> totalPayouts;
    }
    
    @SuppressWarnings("unchecked")
	public BtcTransactionReport loadTransactions() {
		File transactions = new File(websiteLocation, TRANSACTIONS_FILE);
		if(transactions.exists()) {
			try {
				BtcTransactionReport rep = new BtcTransactionReport();
				rep.mapTransactions = (Map<String, BtcTransactionsMonth>) new Gson().fromJson(new FileReader(transactions), 
						rep.mapTransactions.getClass());
				for(Map.Entry<String, BtcTransactionsMonth> key : rep.mapTransactions.entrySet()) {
					BtcTransactionsMonth t = key.getValue();
					t.month = key.getKey();
					rep.txs.add(t);
				}
				for (BtcTransactionsMonth t : rep.txs) {
					loadPayouts(t);
					rep.total += t.total;
				}
				btcTransactionReport = rep;
			} catch (Exception e) {
				LOGGER.error("Fails to read transactions.json: " + e.getMessage(), e);
			}
		}
		return btcTransactionReport;
	}
    
    public BtcTransactionReport getBitcoinTransactionReport() {
    	if(btcTransactionReport.mapTransactions.isEmpty()) {
    		loadTransactions();
    	}
		return btcTransactionReport;
	}
    
    public void reloadConfigs(List<String> errors) {
    	loadTransactions();
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
				BtcTransactionsMonth txs = loadTransactions().mapTransactions.get(month);
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
		Gson gson = new Gson();
		t.totalPayouts = new HashMap<>();
		t.total = 0;
		for (String tid : t.transactions) {
			BtcTransaction tx = new BtcTransaction();
			tx.id = tid;
			tx.url = "https://blockchain.info/rawtx/" + tid;
			tx.total = 0;
			try {

				Map<?, ?> payoutObjects = gson.fromJson(readJsonUrl(tx.url, tid.toString(), true), Map.class);
				// Map<?, ?> data = (Map<?, ?>) payoutObjects.get("data");
				Map<String, String> ins = new TreeMap<String, String>();
				long totalIn = 0;
				long totalOut = 0;
				List<Map<?, ?>> inputs = (List<Map<?, ?>>) payoutObjects.get("inputs");
				for (Map<?, ?> in: inputs) {
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
				tx.blockIndex = ((Number) payoutObjects.get("block_index")).intValue();
				
				t.fee += tx.fee;
				t.total += tx.total;
			} finally {
				if (tx.blockIndex <= 0) {
					getCacheFile(tid).delete();
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
		return new File(websiteLocation, TXS_FOLDER + "/btc_" + id);
	}



}
