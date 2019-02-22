package net.osmand.server.controllers.user;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletResponse;

import net.osmand.server.api.repo.LotterySeriesRepository;
import net.osmand.server.api.repo.LotterySeriesRepository.LotterySeries;
import net.osmand.server.api.repo.LotterySeriesRepository.LotteryStatus;
import net.osmand.server.api.services.DownloadIndexesService;
import net.osmand.server.api.services.DownloadIndexesService.DownloadProperties;
import net.osmand.server.api.services.EmailSenderService;
import net.osmand.server.api.services.IpLocationService;
import net.osmand.server.api.services.LogsAccessService;
import net.osmand.server.api.services.LogsAccessService.LogsPresentation;
import net.osmand.server.api.services.MotdService;
import net.osmand.server.api.services.MotdService.MotdSettings;
import net.osmand.server.api.services.PollsService;
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
	
	@Autowired
	private IpLocationService locationService;
	
	@Autowired
	private LotterySeriesRepository seriesRepo;
	
	@Autowired
	private EmailSenderService emailSender;

	@Autowired
	private LogsAccessService logsAccessService;
	
	private static final String GIT_LOG_CMD = "git log -1 --pretty=format:\"%h%x09%an%x09%ad%x09%s\"";
	private static final SimpleDateFormat timeInputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
	
	
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

	
	
	@RequestMapping(path = { "/register-giveaway" }, method = RequestMethod.POST)
	public String registerGiveaway(Model model,
			@RequestParam(required = true) String name, 
			@RequestParam(required = true) String type, 
			@RequestParam(name="public", required = false) String asPublic,
			@RequestParam(required = true) String emailTemplate,
			@RequestParam(required = true) String promocodes, 
			final RedirectAttributes redirectAttrs) throws JsonProcessingException {
		// seriesRepo
		LotterySeries lotterySeries = new LotterySeries();
		lotterySeries.name = String.format("%1$tY-%1$tm", new Date()) + "-" + name.trim();
		lotterySeries.promocodes = promocodes.trim();
		lotterySeries.emailTemplate = emailTemplate.trim();
		lotterySeries.rounds = "on".equals(asPublic) ? lotterySeries.promocodesSize() : 0;
		lotterySeries.status = "on".equals(asPublic) ? LotteryStatus.DRAFT :
			LotteryStatus.NOTPUBLIC;
		lotterySeries.type = type;
		lotterySeries.updateTime = new Date();
		if(seriesRepo.existsById(lotterySeries.name)) {
			throw new IllegalStateException("Giveaway already exists");
		}
		seriesRepo.save(lotterySeries);
		
		redirectAttrs.addFlashAttribute("update_status", "OK");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", "Givewaway registered");
        return "redirect:info#giveaway";
	}
	

	@RequestMapping(path = { "/update-giveaway-status" }, method = RequestMethod.POST)
	public String updateStatusGiveaway(Model model,
			@RequestParam(required = true) String name, 
			@RequestParam(required = true) String status, 
			final RedirectAttributes redirectAttrs) throws JsonProcessingException {
		// seriesRepo
		Optional<LotterySeries> obj = seriesRepo.findById(name);
		obj.get().status =  LotteryStatus.valueOf(status.toUpperCase());
		seriesRepo.save(obj.get());
		
		redirectAttrs.addFlashAttribute("update_status", "OK");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", "Givewaway registered");
        return "redirect:info#giveaway";
	}
	
	
	@RequestMapping(path = { "/send-private-giveaway" }, method = RequestMethod.POST)
	public String sendGiveaway(Model model,
			@RequestParam(required = true) String name, 
			@RequestParam(required = true) String email, 
			@RequestParam(required = true) int promocodes, 
			final RedirectAttributes redirectAttrs) throws JsonProcessingException {
		// seriesRepo
		Optional<LotterySeries> obj = seriesRepo.findById(name);
		if(!obj.isPresent() || obj.get().status != LotteryStatus.NOTPUBLIC) {
			throw new IllegalArgumentException("Illegal giveaway name is specified");
		}
		LotterySeries s = obj.get();
		Set<String> nonUsedPromos = s.getNonUsedPromos();
		List<String> promos = new ArrayList<String>();
		Iterator<String> it = nonUsedPromos.iterator();
		while(promocodes > 0 && it.hasNext()) {
			String p = it.next();
			promos.add(p);
			s.usePromo(p);
			promocodes--;
		}
		boolean sent = emailSender.sendPromocodesEmails(email, s.emailTemplate, String.join(",", promos));
		seriesRepo.save(s);
		
		redirectAttrs.addFlashAttribute("update_status", sent ? "OK" : "Something went wrong!");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", sent ? "Givewaway sent" : "Something went wrong!");
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
	
	
	
		
	@RequestMapping("/access-logs")
	public void loadLogs(@RequestParam(required=false) String starttime, 
			@RequestParam(required = false) String endtime,
			@RequestParam(required = false) String region,
			@RequestParam(required = false) String filter,
			@RequestParam(required = false) int limit,
			@RequestParam(required = false) String gzip,
			@RequestParam(required = false) String behavior,
			@RequestParam(required = false) String stats,
			HttpServletResponse response) throws SQLException, IOException, ParseException {
		Date startTime = starttime != null && starttime.length() > 0 ? timeInputFormat.parse(starttime) : null;
		Date endTime = endtime != null && endtime.length() > 0 ? timeInputFormat.parse(endtime) : null;
		boolean parseRegion = "on".equals(region);
		boolean behaviorAnalysis = "on".equals(behavior);
		boolean statAnalysis = "on".equals(stats);
		
		boolean gzipFlag = "on".equals(gzip);
		OutputStream out;
		if(gzipFlag) {
			if(behaviorAnalysis) {
				response.setHeader("Content-Disposition", "attachment; filename=logs.json.gz");
			} else {
				response.setHeader("Content-Disposition", "attachment; filename=logs.csv.gz");
			}
			response.setHeader("Content-Type", "application/x-gzip");
			out = new GZIPOutputStream(response.getOutputStream());
		} else {
			out = response.getOutputStream();
		}
		LogsPresentation presentation = LogsPresentation.PLAIN;
		if(behaviorAnalysis) {
			presentation = LogsPresentation.BEHAVIOR;
		} else if(statAnalysis) {
			presentation = LogsPresentation.STATS;
		}
		logsAccessService.parseLogs(startTime, endTime, parseRegion, limit, filter, presentation, out);
		response.flushBuffer();
		response.getOutputStream().close();
		
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
		settings = motdService.getSubscriptionSettings();
		if (settings != null) {
			model.addAttribute("subSettings", settings);
		}
		model.addAttribute("giveaways", seriesRepo.findAllByOrderByUpdateTimeDesc());
		model.addAttribute("downloadServers", getDownloadSettings());
		model.addAttribute("reports", getReports());
		model.addAttribute("surveyReport", getSurveyReport());
		model.addAttribute("subscriptionsReport", getSubscriptionsReport());
		model.addAttribute("emailsReport", getEmailsDBReport());
		model.addAttribute("newSubsReport", getNewSubsReport());
		model.addAttribute("futureCancelReport", getFutureCancelReport());
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
		public int quarterCount;
		public int annualCount;
		public int annualDiscountCount;
		
		public int total;
		public int totalWeighted;
		public int totalGain;
		
		public int cancelMonthCount;
		public int cancelQuarterCount;
		public int cancelAnnualCount;
		public int cancelAnnualDiscountCount;
		
		public int cancelTotal;
		public int cancelTotalWeighted;
		public int cancelLoss;
		
		public int delta;
		public int deltaWeighted;
	}
	
	private List<NewSubscriptionReport> getFutureCancelReport() {
		List<NewSubscriptionReport> result = jdbcTemplate
				.query(
						"select date_trunc('day', expiretime) d,  count(*) cnt from supporters_device_sub where   " +
						"expiretime > now() +  interval '1 days' and expiretime < now() +  interval '40 days' " +
						"group by date_trunc('day', expiretime) order by 1 asc" , new RowMapper<NewSubscriptionReport>() {

					@Override
					public NewSubscriptionReport mapRow(ResultSet rs, int rowNum) throws SQLException {
						NewSubscriptionReport sr = new NewSubscriptionReport();
						sr.date = String.format("%1$tF", rs.getDate(1)); 
						sr.cancelMonthCount = rs.getInt(2);
						return sr;
					}

				});
		return result;
	}	
	
	private List<NewSubscriptionReport> getNewSubsReport() {
		List<SubscriptionReport> cancelled = jdbcTemplate
				.query(	"SELECT O.d, A.cnt, A.sku from ( " +
						"	SELECT date_trunc('day', generate_series(now() - '90 days'::interval, now(), '1 day'::interval)) as d" +
						") O left join ( " +
						"	SELECT date_trunc('day', expiretime) d,  count(*) cnt, sku from supporters_device_sub " +
						"	WHERE expiretime < now() - interval '9 hours' and expiretime > now() -  interval '90 days' " +
						"	GROUP BY date_trunc('day', expiretime), sku " +
						") A on A.d = O.d order by 1 desc", getRowMapper());
		mergeSubscriptionReports(cancelled);
		List<SubscriptionReport> newActive = jdbcTemplate
				.query(	"SELECT O.d, A.cnt, A.sku from ( " +
						"	SELECT date_trunc('day', generate_series(now() - '90 days'::interval, now(), '1 day'::interval)) as d" +
						") O left join ( " +
						"	SELECT date_trunc('day', starttime) d,  count(*) cnt, sku from supporters_device_sub  " +
						"	WHERE starttime > now() -  interval '90 days' " +
						"	GROUP BY date_trunc('day', starttime), sku " +
						") A on A.d = O.d order by 1 desc", getRowMapper());
		mergeSubscriptionReports(newActive);
		List<NewSubscriptionReport> result = new ArrayList<AdminController.NewSubscriptionReport>();
		if(newActive.size() == cancelled.size()) {
		for(int i = 0; i < newActive.size(); i++) {
			SubscriptionReport na = newActive.get(i);
			SubscriptionReport ca = cancelled.get(i);
			if(!ca.date.equals(na.date)) {
				continue;
			}
			NewSubscriptionReport sr = new NewSubscriptionReport();
			sr.date = na.date; 
			sr.monthCount = na.monthCount + na.iosMonthCount ;
			sr.annualCount = na.annualCount + na.iosAnnualCount;
			sr.quarterCount = na.quarterCount + na.iosQuarterCount;
			sr.annualDiscountCount = na.annualDiscountCount + na.iosAnnualDiscountCount;
			sr.cancelMonthCount = ca.monthCount + ca.iosMonthCount ;
			sr.cancelAnnualCount = ca.annualCount + ca.iosAnnualCount;
			sr.cancelQuarterCount = ca.quarterCount + ca.iosQuarterCount;
			sr.cancelAnnualDiscountCount = ca.annualDiscountCount + ca.iosAnnualDiscountCount;
			sr.total = sr.monthCount + sr.annualCount + sr.annualDiscountCount + sr.quarterCount;
			sr.cancelTotal = sr.cancelMonthCount + sr.cancelAnnualCount + sr.cancelAnnualDiscountCount + sr.cancelQuarterCount;
			
			sr.totalWeighted = (int) na.annualValue;
			sr.cancelTotalWeighted = (int) ca.annualValue;
			sr.totalGain = (int) na.value;
			sr.cancelLoss = (int) ca.value;
			sr.delta = sr.total - sr.cancelTotal;
			sr.deltaWeighted = sr.totalWeighted - sr.cancelTotalWeighted;
			
			result.add(sr);
		}
		}
		return result;
	}
	
	public static class SubscriptionReport {
		public String date;
		public int count;
		public int annualCount;
		public int monthCount;
		public int annualDiscountCount;
		public int quarterCount;
		public int iosAnnualCount;
		public int iosAnnualDiscountCount;
		public int iosQuarterCount;
		public int iosMonthCount;
		public double annualValue;
		public double value;
		
		public void merge(SubscriptionReport c) {
			count += c.count;
			annualCount += c.annualCount;
			monthCount += c.monthCount;
			annualDiscountCount += c.annualDiscountCount;
			quarterCount += c.quarterCount;
			iosAnnualCount += c.iosAnnualCount;
			iosAnnualDiscountCount += c.iosAnnualDiscountCount;
			iosQuarterCount += c.iosQuarterCount;
			iosMonthCount += c.iosMonthCount;
			annualValue += c.annualValue;
			value += c.value;
			
		}
	}
	
	private void addSubCount(SubscriptionReport sr, int cnt, String sku) {
		double value = 0;
		int periodMonth = 0;
		switch(sku) {
		case "osm_live_subscription_2": sr.monthCount+=cnt; periodMonth = 1; value = 1.2; break; 
		case "osm_free_live_subscription_2": sr.monthCount+=cnt; periodMonth = 1; value = 1.8; break;
		case "osm_live_subscription_annual_free_v1": sr.annualCount+=cnt; periodMonth = 12; value = 8; break;
		case "osm_live_subscription_annual_free_v2": sr.annualDiscountCount+=cnt; periodMonth = 12; value = 4; break;
		case "osm_live_subscription_annual_full_v1": sr.annualCount+=cnt; periodMonth = 12; value = 6; break;
		case "osm_live_subscription_annual_full_v2": sr.annualDiscountCount+=cnt; periodMonth = 12; value = 3; break;
		case "osm_live_subscription_monthly_free_v1": sr.monthCount+=cnt; periodMonth = 1; value = 2; break;
		case "osm_live_subscription_monthly_full_v1": sr.monthCount+=cnt; periodMonth = 1; value = 1.5; break;
		case "osm_live_subscription_3_months_free_v1": sr.quarterCount+=cnt; periodMonth = 3; value = 4; break;
		case "osm_live_subscription_3_months_full_v1": sr.quarterCount+=cnt; periodMonth = 3; value = 3; break;
		case "net.osmand.maps.subscription.monthly_v1": sr.iosMonthCount+=cnt; periodMonth = 1; value = 2; break;
		case "net.osmand.maps.subscription.3months_v1": sr.iosQuarterCount+=cnt; periodMonth = 3; value =4; break;
		case "net.osmand.maps.subscription.annual_v1": sr.iosAnnualCount+=cnt; periodMonth = 12; value = 8; break;
		default: throw new UnsupportedOperationException("Unsupported subscription " + sku);
		};
		sr.annualValue += cnt * (value * (12 / periodMonth));
		sr.value += cnt * value;
	}
	
	
	private List<SubscriptionReport> getSubscriptionsReport() {
		List<SubscriptionReport> result = jdbcTemplate
				.query(  "SELECT date_trunc('day', now() - a.month * interval '1 month'), count(*), t.sku "	+
						 "from  (select generate_series(0, 18) as month) a join supporters_device_sub t  "	+
						 "on  t.expiretime > now()  - a.month * interval '1 month' and t.starttime < now() - a.month * interval '1 month' "	+
						 "group by a.month, t.sku order by 1 desc, 2 desc", getRowMapper());
		mergeSubscriptionReports(result);
		return result;
	}



	private RowMapper<SubscriptionReport> getRowMapper() {
		return new RowMapper<SubscriptionReport>() {

			@Override
			public SubscriptionReport mapRow(ResultSet rs, int rowNum) throws SQLException {
				SubscriptionReport sr = new SubscriptionReport();
				sr.date = String.format("%1$tF", rs.getDate(1));
				String sku = rs.getString(3);
				if (sku != null && sku.length() > 0) {
					int cnt = rs.getInt(2);
					sr.count += cnt;
					addSubCount(sr, cnt, sku);
				}
				return sr;
			}
		};
	}



	private void mergeSubscriptionReports(List<SubscriptionReport> result) {
		Iterator<SubscriptionReport> it = result.iterator();
		if(it.hasNext()) {
			SubscriptionReport prev = it.next();
			while(it.hasNext()) {
				SubscriptionReport c = it.next();
				if(c.date.equals(prev.date)) {
					prev.merge(c);
					it.remove();
				} else {
					prev = c;
				}
			}
		}
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
