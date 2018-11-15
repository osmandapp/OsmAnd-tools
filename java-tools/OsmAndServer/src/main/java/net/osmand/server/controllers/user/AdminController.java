package net.osmand.server.controllers.user;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServletResponse;

import net.osmand.server.api.services.DownloadIndexesService;
import net.osmand.server.api.services.DownloadIndexesService.DownloadProperties;
import net.osmand.server.api.services.MotdService;
import net.osmand.server.api.services.MotdService.MotdSettings;
import net.osmand.server.controllers.pub.PollsService;
import net.osmand.server.controllers.pub.ReportsController;
import net.osmand.server.controllers.pub.WebController;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
@RequestMapping("/admin")
@PropertySource("classpath:git.properties")
public class AdminController {
	private static final Log LOGGER = LogFactory.getLog(AdminController.class);
	private static final String REPORTS_FOLDER = "reports";

	@Autowired
	private MotdService motdService;
	
	@Autowired
	private DownloadIndexesService downloadService;
	
	@Autowired
	private WebController web;
	
	@Autowired
	private ReportsController reports;
	
	@Autowired
	private PollsService pollsService;
	
	@Autowired
	private ApplicationContext appContext;
	
	@Value("${git.commit.format}")
	private String serverCommit;
	
	@Value("${web.location}")
	private String websiteLocation;
	
	@Value("${files.location}")
    private String filesLocation;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;

	protected ObjectMapper mapper;
	
	private static final String GIT_LOG_CMD = "git log -1 --pretty=format:\"%h%x09%an%x09%ad%x09%s\"";
	
	public AdminController() {
		ObjectMapper objectMapper = new ObjectMapper();
        this.mapper = objectMapper;
	}

	@RequestMapping(path = { "/publish" }, method = RequestMethod.POST)
	public String publish(Model model, final RedirectAttributes redirectAttrs) throws JsonProcessingException {
		List<String> errors = publish();
		redirectAttrs.addFlashAttribute("update_status", "OK");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", "Configurations are reloaded");
		redirectAttrs.addFlashAttribute("services", new String[]{"motd", "download"});
        if(!errors.isEmpty()) {
        	redirectAttrs.addFlashAttribute("update_status", "FAILED");
        	redirectAttrs.addFlashAttribute("update_errors", "Errors: " +errors);
        }
        //return index(model);
        return "redirect:info";
	}

	private List<String> publish() {
		List<String> errors = new ArrayList<>();
		runCmd("git pull", new File(websiteLocation), errors);
        motdService.reloadconfig(errors);
        downloadService.reloadConfig(errors);
        web.reloadConfigs(errors);
        reports.reloadConfigs(errors);
		return errors;
	}


	@RequestMapping("/info")
	public String index(Model model) throws SQLException {
			model.addAttribute("server_startup", String.format("%1$tF %1$tR", new Date(appContext.getStartupDate())));
			model.addAttribute("server_commit", serverCommit);
			String commit = runCmd(GIT_LOG_CMD, new File(websiteLocation), null);
			model.addAttribute("web_commit", commit);
			if (!model.containsAttribute("update_status")) {
				model.addAttribute("update_status", "OK");
				model.addAttribute("update_errors", "");
				model.addAttribute("update_message", "");
			}
			MotdSettings settings = motdService.getSettings();
			if (settings != null) {
				model.addAttribute("motdSettings", settings);
			}

			model.addAttribute("downloadServers", getDownloadSettings());
			model.addAttribute("reports", getReports());
			model.addAttribute("surveyReport", getSurveyReport());
			model.addAttribute("subscriptionsReport", getSubscriptionsReport());
			model.addAttribute("emailsReport", getEmailsDBReport());
			model.addAttribute("newSubsReport", getNewSubsReport());
			model.addAttribute("polls", pollsService.getPollsConfig(false));
			return "admin/info";
	}
	
	public static class EmailReport {
		public String category;
		public String categoryId;
		public int totalCount;
		public int activeMarketing;
		public int activeOsmAndLive;
		public int activeNews;
		
		public int filterMarketing;
		public int filterOsmAndLive;
		public int filterNews;
		public int filterAll;
		
		public void addChannel(String channel, int total) {
			totalCount += total;
			if(channel == null || channel.isEmpty()) {
				// skip
			} else if("marketing".equals(channel)) {
				filterMarketing += total;
			} else if("all".equals(channel)) {
				filterAll += total;
			} else if("osmand_live".equals(channel)) {
				filterOsmAndLive += total;
			} else if("news".equals(channel)) {
				filterNews += total;
			} else {
				filterNews += total;
			}
		}
		
		public EmailReport calculateActive() {
			activeMarketing = totalCount - filterAll - filterMarketing;
			activeOsmAndLive = totalCount - filterAll - filterOsmAndLive;
			activeNews = totalCount - filterAll - filterNews;
			return this;
		}
	}
	
	private void addEmailReport(List<EmailReport> er, String category, String categoryId, String table, String mailCol) {
		final EmailReport re = new EmailReport();
		re.category = category;
		re.categoryId = categoryId;
		jdbcTemplate.query("select count(distinct A."+mailCol+"), U.channel from "+table+ " A "
				+ " left join email_unsubscribed U on A."+mailCol+" = U.email "
				+ " where A."+mailCol+" not in (select email from email_blocked ) group by U.channel",
				new RowCallbackHandler() {
					
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						re.addChannel(rs.getString(2), rs.getInt(1));
					}
				});
		
		er.add(re.calculateActive());
	}
	
	
	private List<EmailReport> getEmailsDBReport() {
		List<EmailReport> er = new ArrayList<EmailReport>();
		addEmailReport(er, "Free users with 3 maps", "email_free_users", "email_free_users", "email");
		addEmailReport(er, "OSM editors (OsmAnd Live)", "osm_recipients", "osm_recipients", "email");
		addEmailReport(er, "OsmAnd Live subscriptions", "supporters", "supporters", "useremail");
		return er;
	}

	public static class NewSubscriptionReport {
		public String date;
		public int monthCount;
		public int annualCount;
		public int total;
		public int cancelTotal;
		public int delta;
		public int cancelMonthCount;
		public int cancelAnnualCount;
	}
	
	private List<NewSubscriptionReport> getNewSubsReport() {
		List<NewSubscriptionReport> result = jdbcTemplate
				.query(
								"	select A.d, A.cnt monthSub, B.cnt annualSub, C.cnt expMSub, D.cnt expYSub from ( " +
								"		select date_trunc('day', starttime) d,  count(*) cnt from supporters_device_sub where  " +
								"		starttime > now() -  interval '90 days' and sku not like '%annual%'  " +
								"		group by date_trunc('day', starttime) " +
								"	) A full outer join ( " +
								"		select date_trunc('day', starttime) d,  count(*) cnt from supporters_device_sub where  " +
								"		starttime > now() -  interval '90 days' and sku like '%annual%' " +
								"		group by date_trunc('day', starttime) " +
								"	) B on B.d = A.d full outer join ( " +
								"		select date_trunc('day', expiretime) d,  count(*) cnt from supporters_device_sub where  " +
								"		expiretime < now() -  interval '3 days' and expiretime > now() -  interval '90 days' and sku not like '%annual%'  " +
								"		group by date_trunc('day', expiretime) " +
								"	) C on C.d = A.d full outer join ( " +
								"		select date_trunc('day', expiretime) d,  count(*) cnt from supporters_device_sub where  " +
								"		expiretime < now() -  interval '3 days' and expiretime > now() -  interval '90 days' and sku like '%annual%' " +
								"		group by date_trunc('day', expiretime) " +
								"	) D on D.d = A.d order by 1 desc", new RowMapper<NewSubscriptionReport>() {

					@Override
					public NewSubscriptionReport mapRow(ResultSet rs, int rowNum) throws SQLException {
						NewSubscriptionReport sr = new NewSubscriptionReport();
						sr.date = rs.getString(1);
						sr.monthCount = rs.getInt(2);
						sr.annualCount = rs.getInt(3);
						sr.cancelMonthCount = rs.getInt(4);
						sr.cancelAnnualCount = rs.getInt(5);
						sr.total = sr.monthCount + sr.annualCount;
						sr.cancelTotal = sr.cancelMonthCount + sr.cancelAnnualCount;
						sr.delta = sr.total - sr.cancelTotal;
						return sr;
					}

				});
		return result;
	}
	
	public static class SubscriptionReport {
		public String date;
		public int count;
	}
	
	
	private List<SubscriptionReport> getSubscriptionsReport() {
		List<SubscriptionReport> result = jdbcTemplate
				.query("SELECT date_trunc('day', now() - a.month * interval '1 month'), count(*) from "
						+ "(select generate_series(0, 12) as month) a join supporters_device_sub t "
						+ " on  t.expiretime > now()  - a.month * interval '1 month' and t.starttime < now() -  a.month * interval '1 month'"
						+ " group by a.month order by 1 desc", new RowMapper<SubscriptionReport>() {

					@Override
					public SubscriptionReport mapRow(ResultSet rs, int rowNum) throws SQLException {
						SubscriptionReport sr = new SubscriptionReport();
						sr.date = String.format("%1$tF", rs.getDate(1));
						sr.count = rs.getInt(2);
						return sr;
					}

				});
		return result;
	}
	
	private static class SurveyReport {
		public String date;
		public int goodCount;
		public int averageCount;
		public int badCount;

		public boolean merge(SurveyReport s) {
			if (this.date.equals(s.date)) {
				this.goodCount += s.goodCount;
				this.averageCount += s.averageCount;
				this.badCount += s.badCount;
				return true;
			}
			return false;
		}
	}
	 
	private List<SurveyReport> getSurveyReport() {
		List<SurveyReport> result = jdbcTemplate.query(
				"SELECT date_trunc('week', \"timestamp\"), response, count(distinct ip) from email_support_survey "
						+ "group by  date_trunc('week', \"timestamp\"), response order by 1 desc,2 desc",
				new RowMapper<SurveyReport>() {

					@Override
					public SurveyReport mapRow(ResultSet rs, int rowNum) throws SQLException {
						SurveyReport sr = new SurveyReport();
						sr.date = String.format("%1$tF", rs.getDate(1));
						if (rs.getString(2).equals("average")) {
							sr.averageCount = rs.getInt(3);
						} else if (rs.getString(2).equals("bad")) {
							sr.badCount = rs.getInt(3);
						} else if (rs.getString(2).equals("good")) {
							sr.goodCount = rs.getInt(3);
						}
						return sr;
					}

				});
		Iterator<SurveyReport> it = result.iterator();
		SurveyReport p = null;
		while (it.hasNext()) {
			SurveyReport c = it.next();
			if (p != null && p.merge(c)) {
				it.remove();
			} else {
				p = c;
			}

		}
		return result;
	}
	

	private List<Map<String, Object>> getReports() {
		List<Map<String, Object>> list = new ArrayList<>();
		File reports = new File(filesLocation, REPORTS_FOLDER);
		File[] files = reports.listFiles();
		if(files != null && reports.exists()) {
			Arrays.sort(files, new Comparator<File>() {

				@Override
				public int compare(File o1, File o2) {
					return o1.getName().compareTo(o2.getName());
				}
			});
			for(File f : files) {
				if(f.getName().startsWith("report_")) {
					Map<String, Object> mo = new TreeMap<>();
					mo.put("name", f.getName().substring("report_".length()));
					mo.put("date", String.format("%1$tF %1$tR", new Date(f.lastModified())));
					mo.put("fullname", f.getName());
					list.add(mo);		
				}
			}
		}
		return list;
	}

	private String runCmd(String cmd, File loc, List<String> errors) {
		try {
			Process p = Runtime.getRuntime().exec(cmd.split(" "), new String[0], loc);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
//			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String s, commit = null;
			// read the output from the command
			while ((s = stdInput.readLine()) != null) {
				if (commit == null) {
					commit = s;
				}
			}

			p.waitFor();
			return commit;
		} catch (Exception e) {
			String fmt = String.format("Error running %s: %s", cmd, e.getMessage());
			LOGGER.warn(fmt);
			if(errors != null) {
				errors.add(fmt);
			}
			return null;
		}
	}

	private List<Map<String, Object>> getDownloadSettings() {
		DownloadProperties dProps = downloadService.getSettings();
		int ms = dProps.getMainServers().size();
		int hs = dProps.getHelpServers().size();
		int mload = ms == 0 ? 0 : dProps.getMainLoad() / ms;
		int mmload = ms == 0 ? 0 : 100 / ms;
		int hload = hs == 0 ? 0 : (100-dProps.getMainLoad()) / hs;
		List<Map<String, Object>> list = new ArrayList<>();
		for(String s : dProps.getMainServers()) {
			Map<String, Object> mo = new TreeMap<>();
			mo.put("name", s);
			mo.put("mainLoad", mload +"%");
			mo.put("srtmLoad", mmload +"%");
			list.add(mo);
		}
		for(String s : dProps.getHelpServers()) {
			Map<String, Object> mo = new TreeMap<>();
			mo.put("name", s);
			mo.put("mainLoad", hload +"%");
			mo.put("srtmLoad", "0%");
			list.add(mo);
		}
		return list;
	}
	
	@RequestMapping(path = "report")
	@ResponseBody
    public ResponseEntity<Resource> downloadReport(@RequestParam(required=true) String file,
                                               HttpServletResponse resp) throws IOException {
		File fl = new File(new File(filesLocation, REPORTS_FOLDER), file) ;
        HttpHeaders headers = new HttpHeaders();
        // headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", fl.getName()));
        headers.add(HttpHeaders.CONTENT_TYPE, "text/plain");
		return  ResponseEntity.ok().headers(headers).body(new FileSystemResource(fl));
	}
}