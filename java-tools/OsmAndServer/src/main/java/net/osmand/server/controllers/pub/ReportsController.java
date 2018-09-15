package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.http.HttpServletRequest;

import net.osmand.data.changeset.OsmAndLiveReportType;
import net.osmand.data.changeset.OsmAndLiveReports;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/nreports")
public class ReportsController {
    private static final Log LOGGER = LogFactory.getLog(ReportsController.class);


    @Value("${web.location}")
    private String websiteLocation;
    
    @Value("${gen.location}")
    private String genLocation;
    

    @Autowired
    JdbcTemplate jdbcTemplate;
    
    // TODO recipients_by_month (transactions.json + text + underpayd)
    
    // TODO query_month_report
//    header("Content-Description: Json report");
//    header("Content-Disposition: attachment; filename=report-$rregion-".$_REQUEST["month"].".json");
//    header("Content-Type: mime/type");
//    if($_REQUEST["report"] == "total") {
//    	echo json_encode(getTotalReport());	
//    } else {
//    	echo json_encode(getReport($_REQUEST["report"], $rregion));
//    }

    // TODO query_report and query_report.php
    @RequestMapping(path = { "query_report_new"})
    @ResponseBody
	public String helpSpecific(HttpServletRequest request, @RequestParam(required = true) String report,
			@RequestParam(required = false) String month, @RequestParam(required = false) String region) throws SQLException, IOException {
    	OsmAndLiveReports query = new OsmAndLiveReports(jdbcTemplate.getDataSource().getConnection(), month);
    	OsmAndLiveReportType type = null;
    	switch (report) {
		case "all_countries":
			type = OsmAndLiveReportType.COUNTRIES;
			break;
		case "available_recipients":
			type = OsmAndLiveReportType.RECIPIENTS;
			break;
		case "ranking_users_by_month":
			type = OsmAndLiveReportType.USERS_RANKING;
			break;
		case "recipients_by_month":
			type = OsmAndLiveReportType.RECIPIENTS;
			break;
		case "supporters_by_month":
			type = OsmAndLiveReportType.SUPPORTERS;
			break;
		case "total_changes_by_month":
			type = OsmAndLiveReportType.TOTAL_CHANGES;
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
    	if(report.equals("all_contributions_requests")) {
    		return String.format("[%s, %s, %s]", 
    				query.getJsonReport(OsmAndLiveReportType.TOTAL_CHANGES, region),
    				query.getJsonReport(OsmAndLiveReportType.RANKING, region),
    				query.getJsonReport(OsmAndLiveReportType.USERS_RANKING, region));
    	}
    	return query.getJsonReport(type, region);
	}
    
    

}