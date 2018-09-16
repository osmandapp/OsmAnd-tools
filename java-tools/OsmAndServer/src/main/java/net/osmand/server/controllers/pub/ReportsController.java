package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import net.osmand.data.changeset.OsmAndLiveReportType;
import net.osmand.data.changeset.OsmAndLiveReports;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/reports")
public class ReportsController {
    private static final Log LOGGER = LogFactory.getLog(ReportsController.class);


    @Value("${web.location}")
    private String websiteLocation;
    
    @Value("${gen.location}")
    private String genLocation;
    
    @Autowired
    private DataSource dataSource;

    
    // TODO recipients_by_month (transactions.json + text + underpayd)
    // TODO getTotalReport
    
    // TODO query_report and query_report.php
    @RequestMapping(path = { "/query_report_new", "/query_report_new.php", 
    		"/query_month_report", "/query_month_report.php"})
    @ResponseBody
	public String getReport(HttpServletRequest request, HttpServletResponse response, 
			@RequestParam(required = true) String report,
			@RequestParam(required = false) String month, @RequestParam(required = false) String region) throws SQLException, IOException {
    	Connection conn = DataSourceUtils.getConnection(dataSource);
    	
		try {
			response.setHeader("Content-Description", "json report");
			response.setHeader("Content-Disposition",
					String.format("attachment; filename=report-%s-%s.json", month, region));
			response.setHeader("Content-Type", "application/json");
			OsmAndLiveReports query = new OsmAndLiveReports(conn, month);
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
				type = OsmAndLiveReportType.PAYOUTS;
				break;
			case "ranking":
				type = OsmAndLiveReportType.RANKING;
				break;
			default:
				break;
			}
			if (report.equals("all_contributions_requests")) {
				return String.format("[%s, %s, %s]", query.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, region),
						query.getJsonReport(OsmAndLiveReportType.RANKING, region),
						query.getJsonReport(OsmAndLiveReportType.USERS_RANKING, region));
			}
			return query.getJsonReport(type, region);
		} finally {
    		DataSourceUtils.releaseConnection(conn, dataSource);
    	}
	}
    
    

}