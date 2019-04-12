package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import net.osmand.data.changeset.OsmAndLiveReportType;
import net.osmand.data.changeset.OsmAndLiveReports;
import net.osmand.data.changeset.OsmAndLiveReports.RecipientsReport;

@RestController
@RequestMapping("/reports")
public class ReportsController {
    protected static final Log LOGGER = LogFactory.getLog(ReportsController.class);


    @Value("${web.location}")
    private String websiteLocation;
    
    @Value("${gen.location}")
    private String genLocation;
    
    @Autowired
    private DataSource dataSource;
    
    private Map<String, Object> transactionsMap = new HashMap<String, Object>();
    
    @SuppressWarnings("unchecked")
	public Map<String, Object> getTransactions() throws FileNotFoundException {
    	if(!transactionsMap.isEmpty()) {
    		return transactionsMap;
    	}
		File transactions = new File(websiteLocation, "reports/transactions.json");
		transactionsMap = (Map<String, Object>) new Gson().fromJson(new FileReader(transactions), transactionsMap.getClass());
		return transactionsMap;
	}
    
    public void reloadConfigs(List<String> errors) {
    	transactionsMap = new HashMap<String, Object>();
	}

    
    @RequestMapping(path = { "/query_report", "/query_report.php", 
    		"/query_month_report", "/query_month_report.php"})
    @ResponseBody
    @SuppressWarnings("unchecked")
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
				Map<String, Object> txs = (Map<String, Object>) getTransactions().get(month);
				StringBuilder payouts = new StringBuilder(); 
				if(txs != null && txs.get("transactions") != null) {
					List<String> ar = (List<String>) txs.get("transactions");
					if(ar.size() > 0) {
						int i = 1;
						payouts.append("Payouts:&nbsp;");
						for(String s : ar) {
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



}
