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
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletResponse;

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

import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;
import net.osmand.server.api.repo.LotterySeriesRepository;
import net.osmand.server.api.repo.LotterySeriesRepository.LotterySeries;
import net.osmand.server.api.repo.LotterySeriesRepository.LotteryStatus;
import net.osmand.server.api.services.DownloadIndexesService;
import net.osmand.server.api.services.DownloadIndexesService.DownloadProperties;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerSpecialty;
import net.osmand.server.api.services.EmailSenderService;
import net.osmand.server.api.services.IpLocationService;
import net.osmand.server.api.services.LogsAccessService;
import net.osmand.server.api.services.LogsAccessService.LogsPresentation;
import net.osmand.server.api.services.MotdService;
import net.osmand.server.api.services.MotdService.MotdSettings;
import net.osmand.server.api.services.PollsService;
import net.osmand.server.controllers.pub.ReportsController;
import net.osmand.server.controllers.pub.ReportsController.BtcTransactionReport;
import net.osmand.server.controllers.pub.ReportsController.PayoutResult;
import net.osmand.server.controllers.pub.WebController;

@Controller
@RequestMapping("/admin")
@PropertySource("classpath:git.properties")
public class AdminController {
	private static final Log LOGGER = LogFactory.getLog(AdminController.class);
	
	private static final String ACCESS_LOG_REPORTS_FOLDER = "reports";

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
	protected IpLocationService locationService;
	
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
	
	
	@RequestMapping(path = { "/update-btc-report" }, method = RequestMethod.POST)
	public String publish(Model model, 
			@RequestParam(required = false) String defaultFee, 
			@RequestParam(required = false) String waitingBlocks, final RedirectAttributes redirectAttrs) throws JsonProcessingException {
		reports.updateBitcoinReport(defaultFee, waitingBlocks);
		redirectAttrs.addFlashAttribute("update_status", "OK");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", "Bitcoin report is regenerated");
        return "redirect:info#bitcoin";
	}
	
	@RequestMapping(path = { "/make-btc-payout" }, method = RequestMethod.POST)
	public String publish(Model model, 
			@RequestParam(required = true) int batchSize, final RedirectAttributes redirectAttrs) throws IOException {
		BtcTransactionReport rep = reports.getBitcoinTransactionReport();
		PayoutResult res = reports.payOutBitcoin(rep, batchSize);
		if(res.validationError != null) {
			return err(redirectAttrs, res.validationError);
		}
		redirectAttrs.addFlashAttribute("update_status", "OK");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", "Payment successful! Bitcoin transaction id is: " + res.txId);
        return "redirect:info";
	}

	
	
	private String err(RedirectAttributes redirectAttrs, String string) {
		redirectAttrs.addFlashAttribute("update_status", "ERROR");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", string);
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
		
		List<Subscription> allSubs = parseSubscriptions();
		model.addAttribute("subRevenueReportMonth", getRevenueReport(allSubs, true, 24));
		model.addAttribute("subRevenueReportDay", getRevenueReport(allSubs, false, 60));
		
		model.addAttribute("subscriptionsReport", getSubscriptionsReport());
		model.addAttribute("yearSubscriptionsReport", getYearSubscriptionsReport());
		model.addAttribute("emailsReport", getEmailsDBReport());
		model.addAttribute("newSubsReport", getNewSubsReport());
		model.addAttribute("btc", getBitcoinReport());
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
	
	
	private BtcTransactionReport getBitcoinReport() {
//		new File(websiteLocation, BTC_REPORT);
		return reports.getBitcoinTransactionReport();
	}
	private List<EmailReport> getEmailsDBReport() {
		List<EmailReport> er = new ArrayList<EmailReport>();
		addEmailReport(er, "Free users with 3 maps", "email_free_users", "email_free_users", "email");
		addEmailReport(er, "OSM editors (OsmAnd Live)", "osm_recipients", "osm_recipients", "email");
		addEmailReport(er, "OsmAnd Live subscriptions", "supporters", "supporters", "useremail");
		return er;
	}
	
	public static class ActiveSubscriptionReport {
		public String date;
		public SubscriptionReport a; // active 
		public SubscriptionReport n; // new
		public SubscriptionReport c; // cancelled
	}
	
	public static class NewSubscriptionReport {
		public String date;
		public int monthCount;
		public int quarterCount;
		public int annualCount;
		public int annualDiscountCount;
		
		public int iosMonthCount;
		public int iosQuarterCount;
		public int iosAnnualCount;
		public int iosAnnualDiscountCount;
		
		public int total;
		public int atotal;
		public int iostotal;
		public int totalWeighted;
		public int totalAvgDuration;
		
		public int cancelMonthCount;
		public int cancelQuarterCount;
		public int cancelAnnualCount;
		public int cancelAnnualDiscountCount;
		
		public int cancelTotal;
		public int cancelTotalWeighted;
		public int cancelTotalAvgDuration;
		
		public int delta;
		public int deltaWeighted;
		public int revenueGain;
		public int totalLifeValue;
		public int cancelTotalLifeValue;
	}
	
	
	public static class YearSubscriptionReport {
		public String month;
		public int[] iOSRenew;
		public String[] strIOSRenew;
		public int[] iOSNonRenew;
		public String[] strIOSNonRenew;
		public int[] androidV2Renew;
		public String[] strAndroidV2Renew;
		public int[] androidV2NonRenew;
		public String[] strAndroidV2NonRenew;
		public int[] androidV1Renew;
		public String[] strAndroidV1Renew;
		public int[] androidV1NonRenew;
		public String[] strAndroidV1NonRenew;
		
		public int[] totalLost;
		public String[] strTotalLost;
		
		public int[] totalKeep;
		public String[] strTotalKeep;
		
		public int[] allTotal;
		public String[] strAllTotal;
		
		
		public YearSubscriptionReport(String month) {
			this.month = month;
		}
		
		public void plus(YearSubscriptionReport r) {
			iOSNonRenew = addArrayToArray(iOSNonRenew, r.iOSNonRenew);
			iOSRenew = addArrayToArray(iOSRenew, r.iOSRenew);
			androidV2Renew = addArrayToArray(androidV2Renew, r.androidV2Renew);
			androidV2NonRenew = addArrayToArray(androidV2NonRenew, r.androidV2NonRenew);
			androidV1Renew = addArrayToArray(androidV1Renew, r.androidV1Renew);
			androidV1NonRenew = addArrayToArray(androidV1NonRenew, r.androidV1NonRenew);
		}
		
		public void total() {
			totalLost = addArrayToArray(totalLost, iOSNonRenew);
			totalLost = addArrayToArray(totalLost, androidV2NonRenew);
			totalLost = addArrayToArray(totalLost, androidV1NonRenew);
			
			totalKeep = addArrayToArray(totalKeep, iOSRenew);
			totalKeep = addArrayToArray(totalKeep, androidV2Renew);
			totalKeep = addArrayToArray(totalKeep, androidV1Renew);
			
			allTotal = addArrayToArray(allTotal, totalKeep);
			allTotal = addArrayToArray(allTotal, totalLost);

			strIOSNonRenew = total(iOSNonRenew, allTotal);
			strIOSRenew = total(iOSRenew, allTotal);
			strAndroidV2Renew = total(androidV2Renew, allTotal);
			strAndroidV1Renew = total(androidV1Renew, allTotal);
			strAndroidV2NonRenew = total(androidV2NonRenew, allTotal);
			strAndroidV1NonRenew = total(androidV1NonRenew, allTotal);
			strTotalLost = total(totalLost, allTotal);
			strTotalKeep = total(totalKeep, allTotal);
			strAllTotal = total(allTotal, allTotal);
		}

		private String[] total(int[] s, int[] tot) {
			String[] res = new String[3];
			StringBuilder r = new StringBuilder();
			int valsum = 0; 
			int totval = 0;
			for(int kn = 0; kn < tot.length; kn++) {
				totval += tot[kn];
			}
			if (s != null) {
				for (int k = 0; k < s.length; k++) {
					if (k > 0) {
						r.append("+");
					}
					int val = s[k];
					r.append(val);
					valsum += val;
				}
			}
			res[0] = r.toString();
			res[1] = valsum +"";
			if (totval > 0 && valsum > 0) {
				res[2] = ((int) valsum * 1000 / totval) / 10.0 + "%";
			} else {
				res[2] = "";
			}
			return res;
		}
	}
	
	private static int[] addArrayToArray(int[] res, int[] add) {
		if (add == null) {
			return res;
		}
		for (int k = 0; k < add.length; k++) {
			res = addNumberToArr(k + 1, res, add[k]);
		}
		return res;
	}
	
	private static int[] addNumberToArr(int pos1Based, int[] arr, int cnt) {
		int ind = pos1Based - 1;
		if (arr == null) {
			arr = new int[pos1Based];
		}
		if (arr.length <= ind) {
			int[] l = new int[pos1Based];
			System.arraycopy(arr, 0, l, 0, arr.length);
			arr = l;
		}
		arr[ind] += cnt;
		return arr;
	}
 	
	
	private Collection<YearSubscriptionReport> getYearSubscriptionsReport() {
		final Map<String, YearSubscriptionReport> res = new LinkedHashMap<String, YearSubscriptionReport>(); 
		jdbcTemplate.query("select  to_char(starttime, 'YYYY-MM') \"start\", " + 
				"        round(extract(day from expiretime - starttime)/365) \"years\", sku, " + 
				"        count(*) FILTER (WHERE autorenewing and valid) \"auto\", " + 
				"        count(*) FILTER (WHERE not autorenewing or not valid) \"non-auto\"," + 
				"        count(*) FILTER (WHERE autorenewing is null) \"auto-null\" " + 
				"    from supporters_device_sub where sku like '%annual%'  and extract(day from expiretime - starttime) > 180" + 
				"    group by \"start\", \"years\", sku " + 
				"    order by 1 desc, 2, 3;", new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						String month = rs.getString(1);
						int years = rs.getInt(2);
						String sku = rs.getString(3);
						int renewing = rs.getInt(4);
						int nonrenewing = rs.getInt(5);
						int unknown = rs.getInt(6);
						YearSubscriptionReport report  = res.get(month);
						if(report == null) {
							report = new YearSubscriptionReport(month);
							res.put(month, report);
						}
						if(sku.startsWith("net.osmand")) {
							report.iOSRenew = addNumberToArr(years, report.iOSRenew, renewing);
							report.iOSNonRenew = addNumberToArr(years, report.iOSNonRenew, nonrenewing);
							report.iOSNonRenew = addNumberToArr(years, report.iOSNonRenew, unknown);
						} else if(sku.contains("v1")) {
							report.androidV1Renew = addNumberToArr(years, report.androidV1Renew, renewing);
							report.androidV1NonRenew = addNumberToArr(years, report.androidV1NonRenew, nonrenewing);
							report.androidV1NonRenew = addNumberToArr(years, report.androidV1NonRenew, unknown);
						} else if(sku.contains("v2")) {
							report.androidV2Renew = addNumberToArr(years, report.androidV2Renew, renewing);
							report.androidV2NonRenew = addNumberToArr(years, report.androidV2NonRenew, nonrenewing);
							report.androidV2NonRenew = addNumberToArr(years, report.androidV2NonRenew, unknown);
						}
					}

					
		});
		ArrayList<YearSubscriptionReport> list = new ArrayList<>(res.values());
		YearSubscriptionReport totalAll = new YearSubscriptionReport("All");
		Map<String, YearSubscriptionReport> yearsTotal = new TreeMap<String, AdminController.YearSubscriptionReport>();
		for(YearSubscriptionReport r: res.values()) {
			r.total();
			String year = r.month.substring(0, 4);
			YearSubscriptionReport yearSubscriptionReport = yearsTotal.get(year);
			if(yearSubscriptionReport == null) {
				yearSubscriptionReport = new YearSubscriptionReport(year);
				yearsTotal.put(year, yearSubscriptionReport);
			}
			yearSubscriptionReport.plus(r);
			totalAll.plus(r);
		}
		totalAll.total();
		for(YearSubscriptionReport y : yearsTotal.values()) {
			y.total();
			list.add(y);
		}
		list.add(totalAll);
		return list;
	}
	
	
	public static class AdminGenericSubReportColumnValue {
		public int active;
		public int totalNew;
		public int totalOld;
		public int totalEnd;
		public long valueNew;
		public long valueOld;
		public long valueEnd;
		public long valueNewLTV;
		
		@Override
		public String toString() {
			return toString(0);
		}
		
		public String toString(int formatVersion) {
			if (formatVersion == 1) {
				return String.format("%d, € %d<br>€ %d", totalNew, valueNewLTV / 1000, (valueNew + valueOld) / 1000);
			}
			String activeStr = active > 0 ? (active + " ") : "";
			return String.format("%s<b>+%d</b><br>" +
			// "€ %d + € %d<br>"+
					"+%d -%d<br><b>€ %d</b><br>€ %d", 
					activeStr, totalNew, 
					totalOld, totalEnd,
					// valueNew / 1000, valueOld / 1000, valueEnd / 1000,
					(valueNew + valueOld) / 1000, valueNewLTV / 1000);
		}
		
	}
	
	public static class AdminGenericSubReportColumn {
		private Set<SubAppType> filterApp = null;
		private int filterDuration = -1;
		private Boolean discount;
		public final String name;
		
		public AdminGenericSubReportColumn(String name) {
			this.name = name;
		}
		
		public AdminGenericSubReportColumn app(SubAppType... vls) {
			this.filterApp = EnumSet.of(vls[0], vls);
			return this;
		}
		
		public AdminGenericSubReportColumn duration(int duration) {
			this.filterDuration = duration;
			return this;
		}
		public AdminGenericSubReportColumn discount(boolean discount) {
			this.discount = discount;
			return this;
		}
		
		public void process(Subscription sub, AdminGenericSubReportColumnValue value) {
			if(!filter(sub)) {
				return;
			}
			int eurMillis = sub.priceEurMillis;
			if (sub.currentPeriod > 0) {
				value.totalOld++;
				value.valueOld += eurMillis;
			} else if (sub.currentPeriod == 0) {
				value.totalNew++;
				value.valueNewLTV += sub.priceLTVEurMillis;
				value.valueNew += eurMillis;
			} else {
				value.totalEnd++;
				value.valueEnd += eurMillis;
			}
		}

		public boolean filter(Subscription sub) {
			if (filterApp != null && !filterApp.contains(sub.app)) {
				return false;
			}
			if (filterDuration != -1 && filterDuration != sub.durationMonth) {
				return false;
			}
			if (discount != null) {
				boolean d = sub.sku.contains("v2") || sub.introPriceMillis >= 0;
				if (d != discount) {
					return false;
				}
			}
			return true;
		}

		public AdminGenericSubReportColumnValue initValue() {
			return new AdminGenericSubReportColumnValue();
		}

		
	}
	
	public static class AdminGenericSubReport {
		public boolean month; // month or day
		public int count; 
		public List<AdminGenericSubReportColumn> columns = new ArrayList<>();
		public Map<String, List<AdminGenericSubReportColumnValue>> values = new LinkedHashMap<>();

		public void addResult(Subscription s) {
			List<AdminGenericSubReportColumnValue> vls = values.get(month ? s.startPeriodMonth : s.startPeriodDay);
			if (vls != null) {
				for (int i = 0; i < columns.size(); i++) {
					columns.get(i).process(s, vls.get(i));
				}
			}
			if(s.currentPeriod == 0 && month) {
				Calendar c = Calendar.getInstance();
				c.setTimeInMillis(s.startPeriodTime);
				while (c.getTimeInMillis() < s.endTime) {
					vls = values.get(Subscription.monthFormat.format(c.getTime()));
					if (vls != null) {
						for (int i = 0; i < columns.size(); i++) {
							if (columns.get(i).filter(s)) {
								vls.get(i).active++;
							}
						}
					}
					c.add(Calendar.MONTH, 1);
				}
			}
		}
	}
	
	
	private AdminGenericSubReport getRevenueReport(List<Subscription> subs, boolean month, int length) {
		AdminGenericSubReport report = new AdminGenericSubReport();
		report.month = month;
		report.count = length;
		String h = ""; //"<br>New + Renew <br> - Cancel";
		report.columns.add(new AdminGenericSubReportColumn("All"));
		report.columns.add(new AdminGenericSubReportColumn("A Y" + h).app(SubAppType.OSMAND).discount(false).duration(12));
		report.columns.add(new AdminGenericSubReportColumn("A+ Y" + h).app(SubAppType.OSMAND_PLUS).discount(false).duration(12));
		report.columns.add(new AdminGenericSubReportColumn("A/2 Y" + h).app(SubAppType.OSMAND).discount(true).duration(12));
		report.columns.add(new AdminGenericSubReportColumn("A+/2 Y" + h).app(SubAppType.OSMAND_PLUS).discount(true).duration(12));
		report.columns.add(new AdminGenericSubReportColumn("A Q" + h).app(SubAppType.OSMAND).duration(3));
		report.columns.add(new AdminGenericSubReportColumn("A+ Q" + h).app(SubAppType.OSMAND_PLUS).duration(3));
		report.columns.add(new AdminGenericSubReportColumn("A M" + h).app(SubAppType.OSMAND).duration(1));
		report.columns.add(new AdminGenericSubReportColumn("A+ M" + h).app(SubAppType.OSMAND_PLUS).duration(1));
		report.columns.add(new AdminGenericSubReportColumn("I Y" + h).app(SubAppType.IOS).duration(12));
		report.columns.add(new AdminGenericSubReportColumn("I Q" + h).app(SubAppType.IOS).duration(3));
		report.columns.add(new AdminGenericSubReportColumn("I M" + h).app(SubAppType.IOS).duration(1));
		buildReport(report, subs);
		return report;
	}
	
	private static class ExchangeRate {
		private long time;
		private double eurRate;
	}
	
	private static class ExchangeRates {
		Map<String, List<ExchangeRate>> currencies = new LinkedHashMap<>();

		public void add(String cur, long time, double eurRate) {
			ExchangeRate r = new ExchangeRate();
			r.time = time;
			r.eurRate = eurRate;
			if (!currencies.containsKey(cur)) {
				currencies.put(cur, new ArrayList<>());
			}
			currencies.get(cur).add(r);
		}
		
		public double getEurRate(String cur, long time) {
			List<ExchangeRate> lst = currencies.get(cur);
			if (lst != null) {
				ExchangeRate ne = lst.get(0);
				for (int i = 1; i < lst.size(); i++) {
					if (Math.abs(time - lst.get(i).time) < Math.abs(time - ne.time)) {
						ne = lst.get(i);
					}
				}
				return ne.eurRate;
			}
			return 0;
		}
		
	}

	private void buildReport(AdminGenericSubReport report, List<Subscription> subs) {
		SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dateFormat = report.month ? new SimpleDateFormat("yyyy-MM") : dayFormat;
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(System.currentTimeMillis());
		for (int i = 0; i < report.count; i++) {
			List<AdminGenericSubReportColumnValue> lst = new ArrayList<>();
			for(AdminGenericSubReportColumn col : report.columns) {
				lst.add(col.initValue());
			}
			report.values.put(dateFormat.format(c.getTime()), lst);
			c.add(report.month ? Calendar.MONTH : Calendar.DAY_OF_MONTH, -1);
		}
		for(Subscription s : subs) {
			report.addResult(s);
		}
	}


	private List<Subscription> parseSubscriptions() {
		Calendar c = Calendar.getInstance();
		List<Subscription> subs = new ArrayList<AdminController.Subscription>();
		ExchangeRates rates = parseExchangeRates();
		
		jdbcTemplate.query(
				"select sku, price, pricecurrency, coalesce(introprice, -1), starttime, expiretime, autorenewing, valid from supporters_device_sub",
				new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (rs.getDate(5) == null || rs.getDate(6) == null) {
							return;
						}
						Subscription main = createSub(rs);
						if (main.totalPeriods <= 0) {
							return;
						}
						c.setTimeInMillis(main.startTime);
						subs.add(main);
						for (int i = 1; i < main.totalPeriods; i++) {
							Subscription nextPeriod = new Subscription(main);
							c.add(Calendar.MONTH, main.durationMonth);
							nextPeriod.buildUp(c.getTime(), i, rates);
							subs.add(nextPeriod);
						}
						c.setTimeInMillis(main.endTime);
						Subscription endPeriod = new Subscription(main);
						endPeriod.buildUp(c.getTime(), -1, rates);
						subs.add(endPeriod);
					}

					private Subscription createSub(ResultSet rs) throws SQLException {
						Subscription s = new Subscription();
						s.sku = rs.getString(1);
						s.priceMillis = rs.getInt(2);
						s.pricecurrency = rs.getString(3);
						s.introPriceMillis = rs.getInt(4);
						s.startTime = rs.getDate(5).getTime();
						s.endTime = rs.getDate(6).getTime();
						s.autorenewing = rs.getBoolean(7);
						s.valid = rs.getBoolean(8);
						setDefaultSkuValues(s);
						c.setTimeInMillis(s.startTime);
						while(c.getTimeInMillis() < s.endTime) {
							c.add(Calendar.MONTH, 1);
							s.totalMonths++;
						}
						// we rolled up more than 14 days in future 
						if(c.getTimeInMillis() - s.endTime > 1000 * 60 * 60 * 24 * 14) {
							s.totalMonths--;
						}
						s.totalPeriods = (int) Math.round((double) s.totalMonths / s.durationMonth);
						s.buildUp(new Date(s.startTime), 0, rates);
						return s;
					}
				});
		// calculate retention rate
		Map<String, TIntArrayList> skuRetentions = new LinkedHashMap<String, TIntArrayList>();
		for (Subscription s : subs) {
			if (s.currentPeriod == 0) {
				TIntArrayList retentionList = skuRetentions.get(s.getSku());
				if (retentionList == null) {
					retentionList = new TIntArrayList();
					skuRetentions.put(s.getSku(), retentionList);
				}
				boolean ended = s.isEnded();
				while (retentionList.size() < 2 * s.totalPeriods) {
					retentionList.add(0);
				}
				for (int i = 0; i < s.totalPeriods; i++) {
					int ind = 2 * i;
					if (i == s.totalPeriods - 1) {
						if (ended) {
							retentionList.setQuick(ind, retentionList.getQuick(ind) + 1);
						}
					} else {
						retentionList.setQuick(ind, retentionList.getQuick(ind) + 1);
						retentionList.setQuick(ind + 1, retentionList.getQuick(ind + 1) + 1);
					}
				}
			}
		}
		System.out.println("Annual retentions: ");
		for (String s : skuRetentions.keySet()) {
			TIntArrayList arrays = skuRetentions.get(s);
			StringBuilder bld = new StringBuilder();
			double partLeft = 1;
			double sum = 1;
			double retained = 1;
			for(int i = 0; i < arrays.size(); i+=2) {
				int t = arrays.get(i);
				int l = arrays.get(i + 1);
				if(t == 0 || l == 0) {
					break;
				}
				retained = ((double) l) / t;
				partLeft = partLeft * retained;
				sum += partLeft;
				bld.append(((int) (100 * retained))).append("%, ");
			}
			retained = Math.min(retained, 0.9);
			sum += partLeft * retained / (1 - retained); // add tail
			System.out.println(s + " - " + ((float) sum) + " " + bld.toString());
		}
		for(Subscription s : subs) {
			if(s.currentPeriod == 0) {
				s.calculateLTVValue(skuRetentions.get(s.getSku()));
			}
		}
		return subs;
	}


	private ExchangeRates parseExchangeRates() {
		SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
		ExchangeRates rates = new ExchangeRates();
		jdbcTemplate.query("select currency, month, eurrate from exchange_rates", new RowCallbackHandler() {

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				try {
					Date parse = dayFormat.parse(rs.getString(2));
					String cur = rs.getString(1);
					double eurRate = rs.getDouble(3);
					rates.add(cur, parse.getTime(), eurRate);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}

		});
		return rates;
	}
	
	private void setDefaultSkuValues(Subscription s) {
		switch(s.sku) {
		case "osm_free_live_subscription_2": s.app = SubAppType.OSMAND; s.retention = 0.9;  s.defPriceEurMillis = 1800; break;
		case "osm_live_subscription_2": s.app = SubAppType.OSMAND_PLUS; s.retention = 0.9; s.durationMonth = 1; s.defPriceEurMillis = 1200; break;
		
		case "osm_live_subscription_annual_free_v1": s.app = SubAppType.OSMAND; s.retention = 0.65; s.durationMonth = 12; s.defPriceEurMillis = 8000; break;
		case "osm_live_subscription_annual_full_v1": s.app = SubAppType.OSMAND_PLUS; s.retention = 0.73; s.durationMonth = 12; s.defPriceEurMillis = 6000;  break;
		case "osm_live_subscription_annual_free_v2": s.app = SubAppType.OSMAND; s.retention = 0.7; s.durationMonth = 12; s.defPriceEurMillis = 4000; break;
		case "osm_live_subscription_annual_full_v2": s.app = SubAppType.OSMAND_PLUS;  s.retention = 0.81; s.durationMonth = 12; s.defPriceEurMillis = 3000; break;
		case "osm_live_subscription_3_months_free_v1": s.app = SubAppType.OSMAND; s.retention = 0.72; s.durationMonth = 3; s.defPriceEurMillis = 4000; break;
		case "osm_live_subscription_3_months_full_v1": s.app = SubAppType.OSMAND_PLUS; s.retention = 0.83; s.durationMonth = 3; s.defPriceEurMillis = 3000; break;
		case "osm_live_subscription_monthly_free_v1": s.app = SubAppType.OSMAND; s.retention = 0.82; s.durationMonth = 1; s.defPriceEurMillis = 2000; break;
		case "osm_live_subscription_monthly_full_v1": s.app = SubAppType.OSMAND_PLUS;  s.retention = 0.89; s.durationMonth = 1; s.defPriceEurMillis = 1500;  break;

		case "net.osmand.maps.subscription.monthly_v1":s.app = SubAppType.IOS; s.retention = 0.85; s.durationMonth = 1; s.defPriceEurMillis = 2000; break;
		case "net.osmand.maps.subscription.3months_v1": s.app = SubAppType.IOS; s.retention = 0.61; s.durationMonth = 3; s.defPriceEurMillis = 4000; break;
		case "net.osmand.maps.subscription.annual_v1": s.app = SubAppType.IOS; s.retention = 0.65; s.durationMonth = 12; s.defPriceEurMillis = 8000; break;
		default: throw new UnsupportedOperationException("Unsupported subscription " + s.sku);
		};
	}
	
	private List<NewSubscriptionReport> getNewSubsReport() {
		List<SubscriptionReport> cancelled = jdbcTemplate
				.query(	"SELECT O.d, A.cnt, A.sku, A.dur from ( " +
						"	SELECT date_trunc('day', generate_series(now() - '90 days'::interval, now(), '1 day'::interval)) as d" +
						") O left join ( " +
						"	SELECT date_trunc('day', expiretime) d,  count(*) cnt, sku, extract(day FROM sum(expiretime-starttime) ) dur "+
						"   FROM supporters_device_sub " +
						"	WHERE expiretime < now() - interval '9 hours' and expiretime > now() -  interval '90 days' " +
						"	GROUP BY date_trunc('day', expiretime), sku " +
						") A on A.d = O.d order by 1 desc", getRowMapper());
		mergeSubscriptionReports(cancelled);
		List<SubscriptionReport> newActive = jdbcTemplate
				.query(	"SELECT O.d, A.cnt, A.sku, A.dur from ( " +
						"	SELECT date_trunc('day', generate_series(now() - '90 days'::interval, now(), '1 day'::interval)) as d" +
						") O left join ( " +
						"	SELECT date_trunc('day', starttime) d,  count(*) cnt, sku, extract(day FROM sum(expiretime-starttime) ) dur " +
						"   FROM supporters_device_sub  " +
						"	WHERE starttime > now() -  interval '90 days' " +
						"	GROUP BY date_trunc('day', starttime), sku " +
						") A on A.d = O.d order by 1 desc", getRowMapper());
		mergeSubscriptionReports(newActive);
		List<NewSubscriptionReport> result = new ArrayList<AdminController.NewSubscriptionReport>();
		if (newActive.size() == cancelled.size()) {
			for (int i = 0; i < newActive.size(); i++) {
				SubscriptionReport na = newActive.get(i);
				SubscriptionReport ca = cancelled.get(i);
				if (!ca.date.equals(na.date)) {
					continue;
				}
				NewSubscriptionReport sr = new NewSubscriptionReport();
				sr.date = na.date;
				sr.monthCount = na.monthCount;
				sr.iosMonthCount = na.iosMonthCount;
				sr.annualCount = na.annualCount;
				sr.iosAnnualCount = na.iosAnnualCount;
				sr.quarterCount = na.quarterCount;
				sr.iosQuarterCount = na.iosQuarterCount;
				sr.annualDiscountCount = na.annualDiscountCount;
				sr.iosAnnualDiscountCount = na.iosAnnualDiscountCount;
				sr.cancelMonthCount = ca.monthCount + ca.iosMonthCount;
				sr.cancelAnnualCount = ca.annualCount + ca.iosAnnualCount;
				sr.cancelQuarterCount = ca.quarterCount + ca.iosQuarterCount;
				sr.cancelAnnualDiscountCount = ca.annualDiscountCount + ca.iosAnnualDiscountCount;
				sr.atotal = sr.monthCount + sr.annualCount + sr.annualDiscountCount + sr.quarterCount ;
				sr.iostotal = sr.iosMonthCount + sr.iosAnnualCount + sr.iosAnnualDiscountCount + sr.iosQuarterCount ;
				sr.total = sr.atotal + sr.iostotal;
				sr.totalAvgDuration = na.count > 0 ? na.duration / na.count : 0;
				sr.cancelTotal = sr.cancelMonthCount + sr.cancelAnnualCount + sr.cancelAnnualDiscountCount
						+ sr.cancelQuarterCount;
				sr.cancelTotalAvgDuration = ca.count > 0 ? ca.duration / ca.count : 0;

				sr.totalWeighted = (int) na.annualValue;
				sr.cancelTotalWeighted = (int) ca.annualValue;
				sr.totalLifeValue = (int) na.totalLifeValue;
				sr.cancelTotalLifeValue = (int) ca.totalLifeValue;

				sr.revenueGain = (int) (na.value - ca.value);
				sr.delta = sr.total - sr.cancelTotal;
				sr.deltaWeighted = sr.totalWeighted - sr.cancelTotalWeighted;

				result.add(sr);
			}
		}
		return result;
	}
	
	public static class SubscriptionMonthReport {
		public String date;
	}
	
	public static enum SubAppType {
		OSMAND_PLUS,
		IOS,
		OSMAND
	}
	public static class Subscription {
		
		public double retention;
		static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
		static SimpleDateFormat monthFormat = new SimpleDateFormat("yyyy-MM");

		public Subscription() {
		}
		
		public String getSku() {
			return sku + (introPeriod ? "-%" : "");
		}

		public Subscription(Subscription s) {
			this.valid = s.valid;
			this.autorenewing = s.autorenewing;
			this.startTime = s.startTime;
			this.endTime = s.endTime;
			this.pricecurrency = s.pricecurrency;
			this.sku = s.sku;
			this.priceMillis = s.priceMillis;
			this.introPriceMillis = s.introPriceMillis;
			this.durationMonth = s.durationMonth;
			this.app = s.app;
			this.defPriceEurMillis = s.defPriceEurMillis;
			this.totalPeriods = s.totalPeriods;
		}
		
		protected int priceMillis;
		protected int introPriceMillis;
		protected boolean valid;
		protected boolean autorenewing;
		protected long startTime;
		protected long endTime;
		protected String pricecurrency;
		protected String sku;
		// from sku
		protected int durationMonth;
		protected SubAppType app;
		protected int defPriceEurMillis;
		
		// period number
		protected int totalMonths;
		protected int totalPeriods;
		
		// current calculated 
		protected int currentPeriod;
		protected boolean introPeriod;
		protected String startPeriodDay;
		protected long startPeriodTime;
		protected String startPeriodMonth;
		protected int fullPriceEurMillis;
		protected int introPriceEurMillis;
		protected int priceEurMillis;
		
		protected int priceLTVEurMillis;
		
		private boolean isEnded() {
			boolean ended = (System.currentTimeMillis() - endTime) >= 1000l * 60 * 60 * 24 * 10;
			return ended; 
		}
		
		public void calculateLTVValue(TIntArrayList retentionsList) {
			for (int i = currentPeriod + 1; i < totalPeriods; i++) {
				priceLTVEurMillis += fullPriceEurMillis;
			}
			if (currentPeriod >= 0) {
				priceLTVEurMillis += introPriceEurMillis;
			}
			// we could take into account autorenewing but retention will change 
			if (!isEnded()) {
				if (2 * totalPeriods + 1 < retentionsList.size() && retentionsList.get(2 * totalPeriods) > 0
						&& retentionsList.get(2 * totalPeriods + 1) > 0) {
					retention = Math.min(0.95, ((double) retentionsList.get(2 * totalPeriods + 1))
							/ retentionsList.get(2 * totalPeriods));
				}
				priceLTVEurMillis += (long) (fullPriceEurMillis * retention / (1 - retention));
			}
		}
		
		public void buildUp(Date time, int period, ExchangeRates rts) {
			this.startPeriodTime = time.getTime();
			this.startPeriodDay = dayFormat.format(time.getTime());
			this.startPeriodMonth = monthFormat.format(time.getTime());
			this.currentPeriod = period;
			
			this.fullPriceEurMillis = defPriceEurMillis;
			this.introPriceEurMillis = defPriceEurMillis;
			if (introPriceMillis >= 0 && priceMillis > 0) {
				introPriceEurMillis = (int) (((double) introPriceMillis * priceEurMillis) / priceMillis);
			}
			double rate = rts.getEurRate(pricecurrency, startPeriodTime);
			if(rate > 0) {
				fullPriceEurMillis = (int) (priceMillis / rate);
				if (introPriceMillis >= 0) {
					introPriceEurMillis =(int) (introPriceMillis / rate);
				} else {
					introPriceEurMillis = (int) (priceMillis / rate);
				}
			}
			this.introPeriod = currentPeriod == 0 && introPriceEurMillis != fullPriceEurMillis;
			if (this.introPeriod) {
				priceEurMillis = introPriceEurMillis;
			} else {
				priceEurMillis = fullPriceEurMillis;	
			}
			
			
		}
		
	}
	
	public static class SubscriptionReport {
		public String date;
		public int count;
		public int duration;
		
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
		public double totalLifeValue;
		public double valueOfAnnuals;
		public double valueOfQuarterly;
		public double valueOfMonthly;
		
		public void merge(SubscriptionReport c) {
			count += c.count;
			duration += c.duration;
			annualCount += c.annualCount;
			monthCount += c.monthCount;
			annualDiscountCount += c.annualDiscountCount;
			quarterCount += c.quarterCount;
			iosAnnualCount += c.iosAnnualCount;
			iosAnnualDiscountCount += c.iosAnnualDiscountCount;
			iosQuarterCount += c.iosQuarterCount;
			iosMonthCount += c.iosMonthCount;
			annualValue += c.annualValue;
			totalLifeValue += c.totalLifeValue;
			valueOfAnnuals += c.valueOfAnnuals;
			valueOfMonthly += c.valueOfMonthly;
			valueOfQuarterly += c.valueOfQuarterly;
			value += c.value;
			
		}
	}
	
	private void addSubCount(SubscriptionReport sr, int cnt, String sku, int duration) {
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
		double revenuePerPeriod = cnt * value;
		sr.annualValue += revenuePerPeriod * (12 / periodMonth);
		sr.value += revenuePerPeriod;
		sr.totalLifeValue += value * duration / (periodMonth * 30.0);
		if(periodMonth == 12) {
			sr.valueOfAnnuals += revenuePerPeriod; 	
		} else if(periodMonth == 3) {
			sr.valueOfQuarterly += revenuePerPeriod;
		} else if(periodMonth == 1) {
			sr.valueOfMonthly += revenuePerPeriod;
		}
	}
	
	
	private List<ActiveSubscriptionReport> getSubscriptionsReport() {
		List<SubscriptionReport> news = jdbcTemplate
				.query(  "SELECT date_trunc('day', now() - a.month * interval '1 month'), count(*), t.sku, extract(day FROM sum(t.expiretime-t.starttime) ) "	+
						 "FROM (select generate_series(0, 24) as month) a join supporters_device_sub t  "	+
						 "ON t.starttime > now()  - (a.month + 1) * interval '1 month' and t.starttime < now() - a.month * interval '1 month' "	+
						 "GROUP BY a.month, t.sku ORDER BY 1 desc, 2 desc", getRowMapper());
		mergeSubscriptionReports(news);
		
		List<SubscriptionReport> cancelled = jdbcTemplate
				.query(  "SELECT date_trunc('day', now() - a.month * interval '1 month'), count(*), t.sku, extract(day FROM sum(t.expiretime-t.starttime) ) "	+
						 "FROM (select generate_series(0, 24) as month) a join supporters_device_sub t  "	+
						 "ON t.expiretime > now()  - (a.month + 1) * interval '1 month' and t.expiretime < now() - a.month * interval '1 month' "	+
						 "GROUP BY a.month, t.sku ORDER BY 1 desc, 2 desc", getRowMapper());
		mergeSubscriptionReports(cancelled);
		
		List<SubscriptionReport> active = jdbcTemplate
				.query(  "SELECT date_trunc('day', now() - a.month * interval '1 month'), count(*), t.sku, extract(day FROM sum(t.expiretime-t.starttime) ) "	+
						 "FROM (select generate_series(0, 24) as month) a join supporters_device_sub t  "	+
						 "ON t.expiretime > now()  - a.month * interval '1 month' and t.starttime < now() - a.month * interval '1 month' "	+
						 "GROUP BY a.month, t.sku ORDER BY 1 desc, 2 desc", getRowMapper());
		mergeSubscriptionReports(active);
		
		for(int i = 0; i < active.size() - 1; i++) {
			SubscriptionReport currentMonth = active.get(i);
			SubscriptionReport prevMonth = active.get(i + 1);
			// store difference of value instead of value
			// the operation could be inverted by restoring from last value
			currentMonth.valueOfAnnuals = currentMonth.valueOfAnnuals - prevMonth.valueOfAnnuals;
			currentMonth.valueOfQuarterly = currentMonth.valueOfQuarterly - prevMonth.valueOfQuarterly;
			currentMonth.valueOfMonthly = currentMonth.valueOfMonthly - prevMonth.valueOfMonthly;
		}
		// calculate revenue by going from the end 
		for(int i = active.size() - 1; i >= 0; i--) {
			SubscriptionReport currentMonth = active.get(i);
			if(i + 12 < active.size()) {
				SubscriptionReport prevYearMonth = active.get(i + 12);
				// prevYearMonth.valueOfAnnuals already has revenue instead of value
				currentMonth.valueOfAnnuals += prevYearMonth.valueOfAnnuals;
			}
		}
		
		for(int i = active.size() - 1; i >= 0; i--) {
			SubscriptionReport currentMonth = active.get(i);
			if(i + 3 < active.size()) {
				SubscriptionReport prevQuarterMonth = active.get(i + 3);
				// prevYearMonth.valueOfAnnuals already has revenue instead of value
				currentMonth.valueOfQuarterly += prevQuarterMonth.valueOfQuarterly;
			}
		}
		
		for(int i = active.size() - 1; i >= 0; i--) {
			SubscriptionReport currentMonth = active.get(i);
			if (i + 1 < active.size()) {
				SubscriptionReport prevMonth = active.get(i + 1);
				// prevYearMonth.valueOfAnnuals already has revenue instead of value
				currentMonth.valueOfMonthly += prevMonth.valueOfMonthly;
			}
		}
		
		List<ActiveSubscriptionReport> res = new ArrayList<ActiveSubscriptionReport>();
		if(news.size() == active.size() && active.size() == cancelled.size()) {
			for(int i = 0; i < active.size(); i++) {
				ActiveSubscriptionReport a = new ActiveSubscriptionReport();
				a.date = active.get(i).date;
				a.a = active.get(i);
				a.n = news.get(i);
				a.c = cancelled.get(i);
				res.add(a);
			}
		}
		
		return res;
	}



	private RowMapper<SubscriptionReport> getRowMapper() {
		return new RowMapper<SubscriptionReport>() {

			@Override
			public SubscriptionReport mapRow(ResultSet rs, int rowNum) throws SQLException {
				SubscriptionReport sr = new SubscriptionReport();
				sr.date = String.format("%1$tF", rs.getDate(1));
				sr.duration += rs.getInt(4);
				String sku = rs.getString(3);
				if (sku != null && sku.length() > 0) {
					int cnt = rs.getInt(2);
					sr.count += cnt;
					addSubCount(sr, cnt, sku, sr.duration);
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
		File reports = new File(filesLocation, ACCESS_LOG_REPORTS_FOLDER);
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
		List<Map<String, Object>> list = new ArrayList<>();
		for(String s : dProps.getServers()) {
			Map<String, Object> mo = new TreeMap<>();
			mo.put("name", s);
			for(DownloadServerSpecialty sp : DownloadServerSpecialty.values()) {
				mo.put(sp.name().toLowerCase(), dProps.getPercent(sp, s) + "%");
			}
			list.add(mo);
		}
		return list;
	}
	
	@RequestMapping(path = "report")
	@ResponseBody
    public ResponseEntity<Resource> downloadReport(@RequestParam(required=true) String file,
                                               HttpServletResponse resp) throws IOException {
		File fl = new File(new File(filesLocation, ACCESS_LOG_REPORTS_FOLDER), file) ;
        HttpHeaders headers = new HttpHeaders();
        // headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", fl.getName()));
        headers.add(HttpHeaders.CONTENT_TYPE, "text/plain");
		return  ResponseEntity.ok().headers(headers).body(new FileSystemResource(fl));
	}
	
	
}
