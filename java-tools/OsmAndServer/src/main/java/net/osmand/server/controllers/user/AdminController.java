package net.osmand.server.controllers.user;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import jakarta.servlet.http.HttpServletResponse;
import jakarta.transaction.Transactional;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import net.osmand.server.PurchasesDataLoader;
import net.osmand.server.WebAccessConfig;
import net.osmand.server.api.repo.*;
import net.osmand.server.api.services.*;
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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase;
import net.osmand.server.api.repo.LotterySeriesRepository.LotterySeries;
import net.osmand.server.api.repo.LotterySeriesRepository.LotteryStatus;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerLoadBalancer;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerRegion;
import net.osmand.server.api.services.DownloadIndexesService.DownloadServerType;
import net.osmand.server.api.services.LogsAccessService.LogsPresentation;
import net.osmand.server.api.services.MotdService.MotdSettings;
import net.osmand.server.controllers.pub.ReportsController;
import net.osmand.server.controllers.pub.ReportsController.BtcTransactionReport;
import net.osmand.server.controllers.pub.ReportsController.PayoutResult;
import net.osmand.server.controllers.pub.WebController;

import static net.osmand.server.api.services.UserSubscriptionService.OSMAND_PROMO_SUBSCRIPTION;

@Controller
@RequestMapping("/admin")
@PropertySource("classpath:git.properties")
public class AdminController {
	private static final Log LOGGER = LogFactory.getLog(AdminController.class);

	private static final String ACCESS_LOG_REPORTS_FOLDER = "reports";
	
	private static final String RELEASES_FOLDER = "osm-releases";

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
	private DeviceSubscriptionsRepository subscriptionsRepository;

	@Autowired
	private CloudUsersRepository usersRepository;

	@Autowired
	private EmailRegistryService emailService;
	
	@Autowired
	protected UserSubscriptionService userSubService;

	@Autowired
	private ApplicationContext appContext;

	@Value("${git.commit.format}")
	private String serverCommit;

	@Value("${osmand.web.location}")
	private String websiteLocation;

	@Value("${osmand.files.location}")
	private String filesLocation;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	protected IpLocationService locationService;

	@Autowired
	private LotterySeriesRepository seriesRepo;
	
	@Autowired
	private PromoCampaignRepository promoCampaignRepository;
	
	@Autowired
	private PluginsService pluginsService;
	
	@Autowired
	PromoService promoService;

    @Autowired
    AdminService adminService;

	@Autowired
	private EmailSenderService emailSender;

	@Autowired
	private LogsAccessService logsAccessService;

	@Autowired
	private WebAccessConfig webAccessConfig;
	
	private Gson gson = new Gson();
	
	private static final String GIT_LOG_CMD = "git log -1 --pretty=format:\"%h%x09%an%x09%ad%x09%s\"";
	private static final SimpleDateFormat timeInputFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
	
	public static final String PROMO_WEBSITE = "promo_website";
	@Autowired
	private PurchasesDataLoader purchasesDataLoader;


	@RequestMapping(path = { "/publish" }, method = RequestMethod.POST)
	public String publish(Model model, final RedirectAttributes redirectAttrs) throws IOException {
		List<String> errors = publish();
		redirectAttrs.addFlashAttribute("update_status", "OK");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", "Configurations are reloaded");
		redirectAttrs.addFlashAttribute("services", new String[]{"motd", "download"});
		if (!errors.isEmpty()) {
			redirectAttrs.addFlashAttribute("update_status", "FAILED");
			redirectAttrs.addFlashAttribute("update_errors", "Errors: " + errors);
		}

		webAccessConfig.reload();
		purchasesDataLoader.reload();
		
        return "redirect:info";
	}

	@RequestMapping(value = "/**")
	public String adminFallback(HttpServletResponse response,
	                            Model model) {
		response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		model.addAttribute("status", 404);
		return "admin/error";
	}

	@RequestMapping(path = {"/security-error"})
	public String handleSecurityErrors(HttpServletResponse response,
	                            Model model) {
		int status = response.getStatus();
		model.addAttribute("status", status);
		return "admin/error";
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
	
	
	
	@PostMapping(path = { "/make-btc-payout" })
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
	
	@PostMapping(path = {"/register-promo"})
	public String registerPromo(@RequestParam String comment, final RedirectAttributes redirectAttrs) {
		PromoService.PromoResponse resp = promoService.createPromoSubscription(comment, PROMO_WEBSITE, null, false);
		redirectAttrs.addFlashAttribute("subscriptions", Collections.singleton(resp.deviceSub));
		return "redirect:info#audience";
	}

	@PostMapping(path = {"/search-subscription"})
	public String searchSubscription(@RequestParam String identifier, final RedirectAttributes redirectAttrs) {
		List<SupporterDeviceSubscription> deviceSubList = adminService.getSubscriptionsByIdentifier(identifier);
		redirectAttrs.addFlashAttribute("subscriptions", deviceSubList);
		redirectAttrs.addFlashAttribute("identifier", identifier);
		return "redirect:info#audience";
	}

	@PostMapping(path = {"/search-inapps"})
	public String searchInapps(@RequestParam String identifier, final RedirectAttributes redirectAttrs) {
		List<SupporterDeviceInAppPurchase> purchases = adminService.getInappsDetailsByIdentifier(identifier);
		redirectAttrs.addFlashAttribute("inapps", purchases);
		return "redirect:info#audience";
	}

	@Transactional
	@PostMapping(path = {"/downgrade-subscription"})
	public ResponseEntity<String> downgradeSubscription(@RequestParam String orderId, @RequestParam String subscriptionType) {
		List<SupporterDeviceSubscription> subscriptions = subscriptionsRepository.findByOrderId(orderId);
		if (subscriptions != null && !subscriptions.isEmpty()) {
			subscriptions.forEach(s -> {
				// downgrade only promo_website
				if (s.sku.equals(subscriptionType) && s.sku.startsWith(OSMAND_PROMO_SUBSCRIPTION)) {
					Calendar c = Calendar.getInstance();
					c.setTimeInMillis(System.currentTimeMillis());
					s.expiretime = c.getTime();
					subscriptionsRepository.save(s);
					CloudUser pu = usersRepository.findByOrderid(s.orderId);
					if (pu != null) {
						userSubService.verifyAndRefreshProOrderId(pu);
					}
				}
			});
		}
		return ResponseEntity.ok("Downgrade successful");
	}
	
	@GetMapping(path = {"/get-subscription-details"})
	@ResponseBody
	public ResponseEntity<SupporterDeviceSubscription> getSubscriptionDetails(@RequestParam String identifier) {
		SupporterDeviceSubscription deviceSub = adminService.getSubscriptionDetailsByIdentifier(identifier);
		return ResponseEntity.ok(deviceSub);
	}

	@PostMapping("/get-email-by-orderId")
	@ResponseBody
	public ResponseEntity<String> getEmailByOrderId(@RequestParam String orderId) {
		CloudUser pu = usersRepository.findByOrderid(orderId);
		if (pu != null) {
			return ResponseEntity.ok().body(pu.email);
		}
		return ResponseEntity.badRequest().body("User is not found.");
	}

	
	@PostMapping(path = { "/search-emails" })
	public String searchEmail(Model model, 
			@RequestParam(required = true) String emailPart, final RedirectAttributes redirectAttrs) {
		redirectAttrs.addFlashAttribute("emailSearch", emailService.searchEmails(emailPart));
        return "redirect:info#audience";
	}
	
	@PostMapping(path = {"/delete-email"})
	@Deprecated
	public String deleteEmail(@RequestParam String email) {
		throw new IllegalStateException("sendOsmRecipientsDeleteEmail() is obsolete"); // a895722f1
	}
	
	@PostMapping(path = {"/ban-by-osmids"})
	@Deprecated
	public String banByOsmids(@RequestParam String osmidList) {
		throw new IllegalStateException("band by osmids is obsolete"); // a895722f1
	}
	
	private String err(RedirectAttributes redirectAttrs, String string) {
		redirectAttrs.addFlashAttribute("update_status", "ERROR");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", string);
		return "redirect:info";
	}


	@PostMapping(path = { "/register-giveaway" })
	public String registerGiveaway(Model model,
			@RequestParam(required = true) String name, 
			@RequestParam(required = true) String type, 
			@RequestParam(name="public", required = false) String asPublic,
			@RequestParam(required = true) String emailTemplate,
			@RequestParam(required = true) String promocodes, 
			final RedirectAttributes redirectAttrs) {
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
			final RedirectAttributes redirectAttrs) {
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
			@RequestParam(required = false) String uriFilter,
			@RequestParam(required = false) String logFilter,
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
		logsAccessService.parseLogs(startTime, endTime, parseRegion, limit, uriFilter, logFilter, presentation, out);
		response.flushBuffer();
		response.getOutputStream().close();
		
	}

	
	
	@RequestMapping("/info")
	public String index(Model model) throws SQLException, IOException {
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
		
		model.addAttribute("promos", promoCampaignRepository.findAllByOrderByStartTimeDesc());
		model.addAttribute("giveaways", seriesRepo.findAllByOrderByUpdateTimeDesc());
		model.addAttribute("downloadServers", getDownloadSettings());
		model.addAttribute("reports", getReports());
		model.addAttribute("surveyReport", getSurveyReport());
		List<Subscription> allSubs = parseSubscriptions();
		model.addAttribute("subRevenueReportYear", getRevenueReport(allSubs, AdminGenericSubReport.YEAR));
		model.addAttribute("subRevenueReportMonth", getRevenueReport(allSubs, AdminGenericSubReport.MONTH));
		model.addAttribute("subRevenueReportDay", getRevenueReport(allSubs, AdminGenericSubReport.DAY));
		
		model.addAttribute("plugins", pluginsService.getPluginsAdminInfo());
		model.addAttribute("yearSubscriptionsReport", getYearSubscriptionsRetentionReport());
		model.addAttribute("emailsReport", emailService.getEmailsDBReport());
		model.addAttribute("btc", getBitcoinReport());
		model.addAttribute("polls", pollsService.getPollsConfig(false));
		
		model.addAttribute("promoSku", PROMO_WEBSITE);
		
		return "admin/info";
	}
	
	@PostMapping(path = { "/upload-plugin-file" }, produces = "application/json")
	public ResponseEntity<String> uploadGpx(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file)
			throws IOException {
		Map<String, ?> res = pluginsService.uploadFile(file);
		return ResponseEntity.ok().body(gson.toJson(res));
	}
	
	@PostMapping(path = {"/delete-plugin-version"})
	public String deletePlugin(@RequestParam String plugin, @RequestParam String version) throws IOException {
		pluginsService.deletePluginVersion(plugin, version);
		return "redirect:info#plugins";
	}
	
	@PostMapping(path = {"/publish-plugin-version"})
	public String publish(@RequestParam String plugin, @RequestParam String version) throws IOException {
		pluginsService.publish(plugin, version);
		return "redirect:info#plugins";
	}
	
	private BtcTransactionReport getBitcoinReport() {
//		new File(websiteLocation, BTC_REPORT);
		return reports.getBitcoinTransactionReport();
	}
	
	
	public static class YearSubRetentionGroup {
		public int[] active;
		public int[] gone;
		public int[] possiblyGone;

		public String html() {
			int total = 0;
			for(int kn = 0; active != null && kn < active.length; kn++) {
				total += active[kn];
			}
			for(int kn = 0; gone != null && kn < gone.length; kn++) {
				total += gone[kn];
			}
			for(int kn = 0; possiblyGone != null && kn < possiblyGone.length; kn++) {
				total += possiblyGone[kn];
			}
			if (total == 0) {
				return "";
			}
			int totalLost = 0; 
			int totalPossiblyGone = 0;
			for (int kn = 0; possiblyGone != null && kn < possiblyGone.length; kn++) {
				totalPossiblyGone += possiblyGone[kn];
			}
			for (int kn = 0; gone != null && kn < gone.length; kn++) {
				totalLost += gone[kn];
			}
			StringBuilder r = new StringBuilder();
			r.append(String.format("<b>%d</b><br> → <b>%d</b> %s" , total, 
					total - totalLost - totalPossiblyGone, percent(total - totalLost - totalPossiblyGone, total)));
			for (int kn = 0; gone != null && kn < gone.length; kn++) {
				if (gone[kn] > 0) {
					r.append("<br>" + (kn + 1) + ". ").append(-gone[kn]).append(percent(gone[kn], total));
				}
			}
			if (totalPossiblyGone > 0) {
				r.append("<br>?. ").append(-totalPossiblyGone).append(percent(totalPossiblyGone, total));
			}
			
			return r.toString();
		}

		private String percent(int valsum, int totval) {
			return " (" + ((int) valsum * 1000 / totval) / 10.0 + "%)";
		}

		public void addNumber(int years, int active, int possiblyGone, int gone) {
			this.active = addNumberToArr(years, this.active, active);
			this.possiblyGone = addNumberToArr(years, this.possiblyGone, possiblyGone);
			this.gone = addNumberToArr(years, this.gone, gone);
		}

		public void addReport(YearSubRetentionGroup r) {
			this.active = addArrayToArray(this.active, r.active);
			this.gone = addArrayToArray(this.gone, r.gone);
			this.possiblyGone = addArrayToArray(this.possiblyGone, r.possiblyGone);
		}
	}
	
	public static class YearSubscriptionRetentionReport {
		public String month;
		public YearSubRetentionGroup ios = new YearSubRetentionGroup();
		public YearSubRetentionGroup iosFull = new YearSubRetentionGroup();
		public YearSubRetentionGroup iosIntro = new YearSubRetentionGroup();
		public YearSubRetentionGroup iosPro = new YearSubRetentionGroup();
		public YearSubRetentionGroup android = new YearSubRetentionGroup();
		public YearSubRetentionGroup androidFull = new YearSubRetentionGroup();
		public YearSubRetentionGroup androidIntro = new YearSubRetentionGroup();
		public YearSubRetentionGroup androidPro = new YearSubRetentionGroup();
		public YearSubRetentionGroup androidV2 = new YearSubRetentionGroup();
		public YearSubRetentionGroup total = new YearSubRetentionGroup();
		
		public YearSubscriptionRetentionReport(String month) {
			this.month = month;
		}
		
		public void plus(YearSubscriptionRetentionReport r) {
			this.ios.addReport(r.ios);
			this.iosFull.addReport(r.iosFull);
			this.iosIntro.addReport(r.iosIntro);
			this.iosPro.addReport(r.iosPro);
			this.android.addReport(r.android);
			this.androidFull.addReport(r.androidFull);
			this.androidIntro.addReport(r.androidIntro);
			this.androidV2.addReport(r.androidV2);
			this.androidPro.addReport(r.androidPro);
			this.total.addReport(r.total);
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
 	
	
	private Collection<YearSubscriptionRetentionReport> getYearSubscriptionsRetentionReport() {
		final Map<String, YearSubscriptionRetentionReport> res = new LinkedHashMap<String, YearSubscriptionRetentionReport>(); 
		jdbcTemplate.query("select  to_char(starttime, 'YYYY-MM') \"start\", \n"
				+ "    round(extract(day from expiretime - starttime)/365) \"years\", \n"
				+ "    sku, introcycles,\n"
				+ "    count(*) FILTER (WHERE valid and     (autorenewing and now() - '8 days'::interval < expiretime)) \"active\",\n"
				+ "    count(*) FILTER (WHERE valid and not (autorenewing and now() - '8 days'::interval < expiretime)) \"possiblygone\",\n"
				+ "    count(*) FILTER (WHERE not valid) \"gone\"\n"
				+ "    from supporters_device_sub \n"
				+ "    where sku like '%annual%' and extract(day from expiretime - starttime) > 180\n"
				+ "    group by \"start\", \"years\", sku, introcycles\n"
				+ "    order by 1 desc, 2, 3, 4;", new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						int ind = 1;
						String month = rs.getString(ind++);
						int years = rs.getInt(ind++);
						String sku = rs.getString(ind++);
						boolean intro = rs.getInt(ind++) > 0; 
						int active = rs.getInt(ind++);
						int possibleGone = rs.getInt(ind++);
						int gone = rs.getInt(ind++);
						YearSubscriptionRetentionReport report  = res.get(month);
						if (years == 0) {
							return;
						}
						if (report == null) {
							report = new YearSubscriptionRetentionReport(month);
							res.put(month, report);
						}
						
						if (sku.startsWith("net.osmand")) {
							report.ios.addNumber(years, active, possibleGone, gone);
							if(sku.contains("pro")) {
								report.iosPro.addNumber(years, active, possibleGone, gone);
							} else if (intro) {
								report.iosIntro.addNumber(years, active, possibleGone, gone);
							} else {
								report.iosFull.addNumber(years, active, possibleGone, gone);
							}
						} else if(sku.contains("pro")) {
							report.android.addNumber(years, active, possibleGone, gone);
							report.androidPro.addNumber(years, active, possibleGone, gone);
						} else if(sku.contains("v1")) {
							report.android.addNumber(years, active, possibleGone, gone);
							if (intro) {
								report.androidIntro.addNumber(years, active, possibleGone, gone);
							} else {
								report.androidFull.addNumber(years, active, possibleGone, gone);
							}
						} else if(sku.contains("v2")) {
							report.android.addNumber(years, active, possibleGone, gone);
							report.androidV2.addNumber(years, active, possibleGone, gone);
						}
						report.total.addNumber(years, active, possibleGone, gone);
					}

					
		});
		ArrayList<YearSubscriptionRetentionReport> list = new ArrayList<>(res.values());
		YearSubscriptionRetentionReport totalAll = new YearSubscriptionRetentionReport("All");
		Map<String, YearSubscriptionRetentionReport> yearsTotal = new TreeMap<String, AdminController.YearSubscriptionRetentionReport>();
		for (YearSubscriptionRetentionReport r : res.values()) {
			String year = r.month.substring(0, 4);
			YearSubscriptionRetentionReport yearSubscriptionReport = yearsTotal.get(year);
			if (yearSubscriptionReport == null) {
				yearSubscriptionReport = new YearSubscriptionRetentionReport(year);
				yearsTotal.put(year, yearSubscriptionReport);
			}
			yearSubscriptionReport.plus(r);
			totalAll.plus(r);
		}
		for (YearSubscriptionRetentionReport y : yearsTotal.values()) {
			list.add(0, y);
		}
		list.add(0, totalAll);
		return list;
	}
	
	
	public static class AdminGenericSubReportColumnValue {
		public int active;
		public int activeRenew;
		public int totalNew;
		public int totalOld;
		public int totalEnd;
		public long valueNew;
		public long valueOld;
		public long valueEnd;
		public long valueNewLTV;
		public long valuePaidLTV;
		public boolean generic;
		
		public AdminGenericSubReportColumnValue(boolean generic) {
			this.generic = generic;
		}
		
		@Override
		public String toString() {
			return toString(0);
		}
		
		public String toString(int formatVersion) {
			if (formatVersion == 0) {
				return String.format("%d, € %d<br>€ %d", totalNew, valueNewLTV / 1000, (valueNew + valueOld) / 1000);
			}
			boolean lvl1 = (formatVersion & (1 << 1)) > 0;
			boolean lvl2 = (formatVersion & (1 << 2)) > 0;
			boolean lvl3 = (formatVersion & (1 << 3)) > 0;
			boolean lvl4 = (formatVersion & (1 << 4)) > 0;
			StringBuilder row = new StringBuilder();
			if (active > 0) {
				if (activeRenew > 0) {
					row.append(String.format("%d (%d%%) <br>", active, activeRenew * 100 / active));
				} else {
					row.append(String.format("%d<br>", active));
				}
			}
			row.append(String.format("<b>+%d</b>&nbsp;-%d", totalNew, totalEnd));
			if (lvl1 && (totalEnd + totalOld) > 0) {
				row.append(String.format("<br>•%d&nbsp;-%d%%", totalOld + totalEnd,
						(totalEnd * 100) / (totalEnd + totalOld)));
			}
			if (lvl2) {
				row.append(String.format("<br><b>€ %d</b>", (valueNew + valueOld) / 1000));
			}
			if (lvl3 || lvl4) {
				if ((generic || lvl4) && valueNewLTV > 0) {
					row.append(
							String.format("<br>€ %d (%d%%)", valueNewLTV / 1000, (valuePaidLTV * 100) / (valueNewLTV)));
				} else {
					row.append(String.format("<br>€ %d", valueNewLTV / 1000));
				}
				row.append(String.format("<br>Old € %d", valueOld / 1000));
			}
			return row.toString();
		}
		
	}
	
	public static class AdminGenericSubReportColumn {
		private Set<SubAppType> filterApp = null;
		private int filterDuration = -1;
		private Boolean discount;
		private Boolean pro;
		private Boolean maps;
		public final String name;
		
		public AdminGenericSubReportColumn(String name) {
			this.name = name;
		}
		
		public boolean isGenericColumn() {
			return filterDuration == -1 && discount == null;
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
		
		public AdminGenericSubReportColumn pro(boolean pro) {
			this.pro = pro;
			return this;
		}
		
		public AdminGenericSubReportColumn maps(boolean maps) {
			this.maps = maps;
			return this;
		}
		
		public void process(Subscription sub, AdminGenericSubReportColumnValue value) {
			if (!filter(sub)) {
				return;
			}
			int eurMillis = sub.priceEurMillis;
			if (sub.currentPeriod > 0) {
				value.totalOld++;
				value.valueOld += eurMillis;
			} else if (sub.currentPeriod == 0) {
				value.totalNew++;
				value.valueNewLTV += sub.priceLTVEurMillis;
				value.valuePaidLTV += sub.priceTotalPaidEurMillis;
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
			if (pro != null && pro.booleanValue() != sub.pro) {
				return false;
			}
			if (maps != null && maps.booleanValue() != sub.maps) {
				return false;
			}
			if (discount != null) {
				boolean subDiscount;
				if (sub.sku.contains("v2")) {
					subDiscount = true; // start from aug 21 (no discount)
				} else {
					subDiscount = (sub.introPriceMillis >= 0 && sub.introPriceEurMillis < sub.fullPriceEurMillis)
							|| sub.introCycles > 0;
//					if (subDiscount && sub.currentPeriod >= sub.introCycles) {
//						subDiscount = false;
//					}
				}
				if (subDiscount != discount) {
					return false;
				}
			}
			return true;
		}

		
	}
	
	public static class AdminGenericSubReport {
		public static final int MONTH = 0;
		public static final int YEAR = 1;
		public static final int DAY = -1;
		public int period; // month == 0 or day == -1 or year == 1
		public int count; 
		public List<AdminGenericSubReportColumn> columns = new ArrayList<>();
		public Map<String, List<AdminGenericSubReportColumnValue>> values = new TreeMap<>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				int s = Integer.compare(o1.length(), o2.length());
				if (s != 0) {
					return s;
				}
				return -o1.compareTo(o2);
			}
		});
		public List<Subscription> subs;
		
		public Map<String, List<AdminGenericSubReportColumnValue>> getValues(int limit) {
			SimpleDateFormat dateFormat = period == MONTH ? Subscription.monthFormat
					: (period == YEAR ? Subscription.yearFormat : Subscription.dayFormat);
			
			Calendar c = Calendar.getInstance();
			c.setTimeInMillis(System.currentTimeMillis());
			for (int i = 0; i < limit; i++) {
				values.put(dateFormat.format(c.getTime()), initColumns());
				c.add(period == MONTH ? Calendar.MONTH
						: (period == YEAR ? Calendar.YEAR : Calendar.DAY_OF_MONTH), -1);
			}
			for (Subscription s : subs) {
				addResult(s, dateFormat);
			}
			return values;
		}

		private List<AdminGenericSubReportColumnValue> initColumns() {
			List<AdminGenericSubReportColumnValue> lst = new ArrayList<>();
			for (AdminGenericSubReportColumn col : columns) {
				AdminGenericSubReportColumnValue vl = new AdminGenericSubReportColumnValue(col.isGenericColumn());
				lst.add(vl);
			}
			return lst;
		}

		public void addResult(Subscription s, SimpleDateFormat dateFormat) {
			String periodId = period == MONTH ? s.startPeriodMonth
					: (period == YEAR ? s.startPeriodYear : s.startPeriodDay); 
			processSub(s, periodId);
			if (s.currentPeriod == 0 && (period == MONTH || period == YEAR)) {
				Calendar c = Calendar.getInstance();
				c.setTimeInMillis(s.startPeriodTime);
				String fperiodId = dateFormat.format(c.getTime());
				boolean last = false;
				for (int k = 0; k <= s.totalMonths && !last; k++) {
					c.add(Calendar.MONTH, 1);
					if (c.getTimeInMillis() > s.endTime) {
						c.setTimeInMillis(s.endTime);
						last = true;
					}
					String nperiodId = dateFormat.format(c.getTime());
					if (fperiodId.equals(nperiodId)) {
						continue;
					}
					fperiodId = nperiodId;
					List<AdminGenericSubReportColumnValue> vls = values.get(fperiodId);
					if (vls != null) {
						for (int i = 0; i < columns.size(); i++) {
							if (columns.get(i).filter(s)) {
								vls.get(i).active++;
								if (s.valid && s.autorenewing) {
									vls.get(i).activeRenew++;
								}
							}
						}
					}
				}
			}
		}

		private List<AdminGenericSubReportColumnValue> processSub(Subscription s, String periodId) {
			List<AdminGenericSubReportColumnValue> vls = values.get(periodId);
			if (vls != null) {
				for (int i = 0; i < columns.size(); i++) {
					columns.get(i).process(s, vls.get(i));
				}
			}
			return vls;
		}
	}
	
	
	private AdminGenericSubReport getRevenueReport(List<Subscription> subs, int period) {
		AdminGenericSubReport report = new AdminGenericSubReport();
		report.period = period;
		report.subs = subs;
		report.columns.add(new AdminGenericSubReportColumn("All"));
		report.columns.add(new AdminGenericSubReportColumn("Market<br>GPlay").app(SubAppType.OSMAND, SubAppType.OSMAND_PLUS));
		report.columns.add(new AdminGenericSubReportColumn("Market<br>IOS").app(SubAppType.IOS));
		report.columns.add(new AdminGenericSubReportColumn("Market<br>Other" ).app(SubAppType.HUAWEI, SubAppType.AMAZON, SubAppType.FASTSPRING));
		
		report.columns.add(new AdminGenericSubReportColumn("Type<br>Maps A").maps(true));
		report.columns.add(new AdminGenericSubReportColumn("Type<br>PRO A").pro(true).duration(12));
		report.columns.add(new AdminGenericSubReportColumn("Type<br>PRO M").pro(true).duration(1));
		report.columns.add(new AdminGenericSubReportColumn("Type<br>Other").pro(false).maps(false));
		
		report.columns.add(new AdminGenericSubReportColumn("Gplay<br>Full").discount(false).app(SubAppType.OSMAND, SubAppType.OSMAND_PLUS));
		report.columns.add(new AdminGenericSubReportColumn("Gplay<br>%%").discount(true).app(SubAppType.OSMAND, SubAppType.OSMAND_PLUS));
		report.columns.add(new AdminGenericSubReportColumn("iOS<br>Full").discount(false).app(SubAppType.IOS));
		report.columns.add(new AdminGenericSubReportColumn("iOS<br>%%").discount(true).app(SubAppType.IOS));
		return report;
	}
	
	private static class ExchangeRate {
		private long time;
		private double eurRate;
	}
	
	protected static class ExchangeRates {
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


	private List<Subscription> parseSubscriptions() {
		Calendar c = Calendar.getInstance();
		List<Subscription> subs = new ArrayList<AdminController.Subscription>();
		ExchangeRates rates = parseExchangeRates();
		Map<String, PurchasesDataLoader.Subscription> subMap = purchasesDataLoader.getSubscriptions();
		
		jdbcTemplate.query(
				"select sku, price, pricecurrency, coalesce(introprice, -1), starttime, expiretime, autorenewing, valid, introcycles from supporters_device_sub",
				new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						if (rs.getDate(5) == null || rs.getDate(6) == null) {
							return;
						}
						Subscription main = createSub(rs);
						if (main == null || main.totalPeriods <= 0) {
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
						if (s.sku.startsWith("promo_")) {
							return null;
						}
						s.priceMillis = rs.getInt(2);
						s.pricecurrency = rs.getString(3);
						s.introPriceMillis = rs.getInt(4);
						s.startTime = rs.getDate(5).getTime();
						s.endTime = rs.getDate(6).getTime();
						s.autorenewing = rs.getBoolean(7);
						s.valid = rs.getBoolean(8);
						s.introCycles = rs.getInt(9);
						setDefaultSkuValues(s, subMap);
						c.setTimeInMillis(s.startTime);
						c.add(Calendar.DAY_OF_YEAR, 20); // sometimes end time shifts 3-5 days first month make 20 days
						while (c.getTimeInMillis() < s.endTime) {
							s.totalMonths++; 
							c.add(Calendar.MONTH, 1);
						}
						s.totalPeriods = (int) Math.round((double) s.totalMonths / s.durationMonth);
						s.buildUp(new Date(s.startTime), 0, rates);
						return s;
					}
				});
		Map<String,TDoubleArrayList> retentionRates = calculateRetentionRates(subs);
		for (Subscription s : subs) {
			if (s.currentPeriod == 0) {
				s.calculateLTVValue(retentionRates);
			}
		}
		return subs;
	}


	private Map<String, TDoubleArrayList> calculateRetentionRates(List<Subscription> subs) {
		// calculate retention rate
		
		Map<String, TIntArrayList> skuRetentions = new TreeMap<>();
		Map<String, TDoubleArrayList> skuResult = new TreeMap<>();
		Map<String, Subscription> skuExamples = new TreeMap<>();
		Map<String, Integer> skuTotal = new TreeMap<>();
		for (Subscription s : subs) {
			if (s.currentPeriod == 0) {
				skuTotal.compute(s.getSku(), (t, u) -> u == null ? 1 : u + 1);
			}
			if (s.currentPeriod == 0 && s.totalPeriods > 0) {
				TIntArrayList retentionList = skuRetentions.get(s.getSku());
				if (retentionList == null) {
					retentionList = new TIntArrayList();
					skuRetentions.put(s.getSku(), retentionList);
					skuExamples.put(s.getSku(), s);
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
		Map<String, Double> finalTable = new HashMap<String, Double>();
		for (String sku : skuRetentions.keySet()) {
			TIntArrayList arrays = skuRetentions.get(sku);
			Subscription sub = skuExamples.get(sku);
			TDoubleArrayList actualRetention = new TDoubleArrayList();
			for (int i = 0; i < arrays.size(); i += 2) {
				int total = arrays.get(i);
				int left = arrays.get(i + 1);
				if (total == 0 || left == 0) {
					break;
				}
				actualRetention.add(((double) left) / total);
			}
			boolean trim = trimRetention(actualRetention, sub.durationMonth > 1 ? (sub.durationMonth > 3 ? 2 : 3) : 6);
			StringBuilder bld = new StringBuilder();
			double sum = 1;
			if (sku.endsWith("-%")) {
				sum = 0.5;
			}
			double prod = 1;
			double last = sub.retention;
			for (int i = 0; i < actualRetention.size(); i++) {
				last = actualRetention.get(i);
				prod *= last;
				sum += prod;
				if (i == actualRetention.size() - 1 && trim) {
					bld.append(String.format("... %.0f%%", 100 * (double) actualRetention.get(i)));
				} else {
					bld.append(String.format("%.0f%%, ", 100 * (double) actualRetention.get(i)));
				}
			}
			// add up tail
			last = Math.min(last, 0.8);
			sum += prod  * last / (1 - last); // add tail
			
			
			double ltv = sub.defPriceEurMillis / 1000 * sum;
			String msg = String.format("%d %.0f$ - %s = %.1f$ * %.2f [%d%%] : %s", skuTotal.get(sku), ltv, sku, sub.defPriceEurMillis / 1000.0, sum, 
					(int) (sub.retention * 100), bld.toString());
			finalTable.put(msg, skuTotal.get(sku) * ltv);
			actualRetention.insert(0, sum);
			skuResult.put(sku, actualRetention);
		}
		ArrayList<String> msgs = new ArrayList<>(finalTable.keySet());
		Collections.sort(msgs, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return -Double.compare(finalTable.get(o1), finalTable.get(o2));
			}
		});
		System.out.println("Annual retentions (MOVE TO WEB PAGE): ");
		System.out.println("ALL * (Life Time Value) - SKU - Price * Period (Years, Months = LTV) [Active %]: Stats by year");
		System.out.println("---------------");
		
		for (String msg : msgs) {
			System.out.println(msg);
		}
		System.out.println("---------------");
		return skuResult;
	}


	private boolean trimRetention(TDoubleArrayList actualRetention, int RET_PERIOD_TRIM) {
		boolean trim = actualRetention.size() > RET_PERIOD_TRIM;
		if (trim) {
			int cnt = actualRetention.size() - RET_PERIOD_TRIM;
			double prodAverage = 1;
			for (int i = actualRetention.size() - 1; i >= RET_PERIOD_TRIM; i--) {
				double vl = actualRetention.removeAt(i);
				prodAverage *= vl;
			}
			actualRetention.add(Math.pow(prodAverage, 1.0 / cnt));
		}
		return trim;
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

	private void setDefaultSkuValues(Subscription s, Map<String, PurchasesDataLoader.Subscription> subMap) {
		PurchasesDataLoader.Subscription subBaseData = subMap.get(s.sku);
		if (subBaseData == null) {
			throw new UnsupportedOperationException("Unsupported subscription " + s.sku);
		}
		s.app = SubAppType.fromString(subBaseData.app());
		s.pro = subBaseData.isPro();
		s.maps = subBaseData.isMaps();
		s.retention = subBaseData.retention();
		s.durationMonth = subBaseData.duration();
		s.defPriceEurMillis = subBaseData.defaultPriceEurMillis();
	}
	
	
	public static class SubscriptionMonthReport {
		public String date;
	}
	
	public enum SubAppType {
		OSMAND_PLUS,
		IOS,
		OSMAND,
		AMAZON,
		HUAWEI,
		FASTSPRING;

		public static SubAppType fromString(String app) {
			return switch (app.toLowerCase()) {
				case "osmand+" -> OSMAND_PLUS;
				case "osmand" -> OSMAND;
				case "ios" -> IOS;
				case "amazon" -> AMAZON;
				case "huawei" -> HUAWEI;
				case "fastspring" -> FASTSPRING;
				default -> throw new IllegalArgumentException("Unknown app type: " + app);
			};
		}
	}
	public static class Subscription {
		
		public double retention;
		static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
		static SimpleDateFormat monthFormat = new SimpleDateFormat("yyyy-MM");
		static SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy") ;

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
			this.priceEurMillis = s.priceEurMillis;
			this.introPriceEurMillis = s.introPriceEurMillis;
			this.introCycles = s.introCycles;
			this.durationMonth = s.durationMonth;
			this.pro = s.pro;
			this.maps = s.maps;
			this.app = s.app;
			this.defPriceEurMillis = s.defPriceEurMillis;
			this.totalPeriods = s.totalPeriods;
		}
		
		protected int priceMillis;
		protected int introCycles;
		protected int introPriceMillis;
		protected boolean valid;
		protected boolean autorenewing;
		protected long startTime;
		protected long endTime;
		protected String pricecurrency;
		protected String sku;
		// from sku
		protected int durationMonth;
		protected boolean pro;
		protected boolean maps;
		protected SubAppType app;
		protected int defPriceEurMillis;
		
		// period number
		protected int totalMonths;
		protected int totalPeriods;
		
		// current calculated 
		protected int currentPeriod;
		protected boolean introPeriod;
		protected long startPeriodTime;
		protected String startPeriodDay;
		protected String startPeriodMonth;
		protected String startPeriodYear;
		protected int fullPriceEurMillis;
		protected int introPriceEurMillis;
		protected int priceEurMillis;
		
		protected int priceLTVEurMillis;
		protected int priceTotalPaidEurMillis;
		
		private boolean isEnded() {
			boolean ended = (System.currentTimeMillis() - endTime) >= 1000l * 60 * 60 * 24 * 10;
			return ended; 
		}
		
		public void calculateLTVValue(Map<String, TDoubleArrayList> retentionRates) {
			priceTotalPaidEurMillis = introPriceEurMillis;
			if (introCycles > 1) {
				LOGGER.error("Subscription has intro cycles > 1, which is not supported for LTV calculation, sku: " + sku);
				// TODO: fix this
				// throw new UnsupportedOperationException();
			}
			for (int i = 1; i < totalPeriods; i++) {
				priceTotalPaidEurMillis += fullPriceEurMillis;
			}
			int methodLTV = 3;
			TDoubleArrayList rates = retentionRates.get(getSku());
			if (methodLTV == 1) {
				priceLTVEurMillis = priceTotalPaidEurMillis;
				// we could take into account autorenewing but retention will change
				if (!isEnded()) {
					priceLTVEurMillis += (long) (fullPriceEurMillis * retention / (1 - retention));
				}
			} else if (methodLTV == 2) {
				priceLTVEurMillis = (int) (fullPriceEurMillis * rates.get(0));
			} else if (methodLTV == 3) {
				priceLTVEurMillis = priceTotalPaidEurMillis;
				if (!isEnded() && autorenewing) {
					double p = 1;
					for(int i = totalPeriods; i < rates.size() - 1; i ++) {
						p *= rates.get(i);
						priceLTVEurMillis += fullPriceEurMillis * p;
					}
					double lastRetention = Math.min(0.95, rates.size() > 1 ? rates.get(rates.size() - 1) : retention);
					priceLTVEurMillis += (long) (fullPriceEurMillis * lastRetention / (1 - lastRetention)) * p;
				}
			}
		}
		
		public void buildUp(Date time, int period, ExchangeRates rts) {
			this.startPeriodTime = time.getTime();
			this.startPeriodDay = dayFormat.format(time.getTime());
			this.startPeriodMonth = monthFormat.format(time.getTime());
			this.startPeriodYear = yearFormat.format(time.getTime());
			this.currentPeriod = period;
			
			this.fullPriceEurMillis = defPriceEurMillis;
			this.introPriceEurMillis = defPriceEurMillis;
			if (this.introCycles > 0) {
				this.introPriceEurMillis = defPriceEurMillis / 2;
			}
			// TOO old
			double rate = 0;//rts.getEurRate(pricecurrency, startPeriodTime); 
			if (introPriceMillis >= 0 && priceMillis > 0 && rate == 0) {
				rate = priceMillis * 1.0 / defPriceEurMillis;
			}
			if (rate > 0) {
				fullPriceEurMillis = (int) (priceMillis / rate);
				if (introPriceMillis >= 0) {
					introPriceEurMillis = (int) (introPriceMillis / rate);
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

	
	private Map<String, Object> getDownloadSettings() {
		DownloadServerLoadBalancer dProps = downloadService.getSettings();
		List<DownloadServerRegion> regions = new ArrayList<>(dProps.getRegions());
		regions.add(0, dProps.getGlobalRegion());
		List<Object> regionResults = new ArrayList<Object>();
		Set<String> types = new LinkedHashSet<>();
		for (DownloadServerRegion region : regions) {
			List<Map<String, Object>> servers = new ArrayList<>();
			for (String serverName : region.getServers()) {
				Map<String, Object> mo = new TreeMap<>();
				mo.put("name", serverName);
				for (DownloadServerType sp : dProps.getServerTypes()) {
					types.add(sp.key);
					mo.put(sp.key, String.format("%d (%d%%)", region.getDownloadCounts(sp, serverName),
							region.getPercent(sp, serverName)));
				}
				servers.add(mo);
			}
			regionResults.add(Map.of("name", region.toString(), "servers", servers));
		}
		return Map.of("regions", regionResults, "types", types,
				"freemaps", dProps.getFreemaps());
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


	@PostMapping(path = {"/register-promo-campaign"})
	public String registerPromoCampaign(@RequestParam String name,
	                            @RequestParam int subActiveMonths,
	                            @RequestParam int numberLimit,
	                            @RequestParam String endTime,
	                            final RedirectAttributes redirectAttrs) throws ParseException {
		
		PromoCampaignRepository.Promo promo = new PromoCampaignRepository.Promo();
		promo.name = name;
		promo.subActiveMonths = subActiveMonths;
		promo.numberLimit = numberLimit;
		promo.startTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		promo.endTime = formatter.parse(endTime);
		
		promoService.register(promo);
		
		redirectAttrs.addFlashAttribute("update_status", "OK");
		redirectAttrs.addFlashAttribute("update_errors", "");
		redirectAttrs.addFlashAttribute("update_message", "Promo registered");
		
		return "redirect:info#promo";
	}
	
	public static class ReleaseInfo {
		
		public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		public String name;
		public String size;
		public String date;
		public long timestamp;
		public String version;
		public String type;
		public long length;
		public ReleaseInfo(File f) {
			name = f.getName();
			size = String.format("%.2f MB", f.length() / 1024.0 / 1024.0);
			length = f.length();
			timestamp = f.lastModified();
			date = sdf.format(new Date(f.lastModified()));
			int i1 = name.indexOf('-');
			int i2 = name.indexOf('-', i1 + 1);
			if (i2 > 0) {
				version = name.substring(i1 + 1, i2);
			}
		}
	}
	@GetMapping(path = {"/releases"})
	public String releasesPage(Model model) {
		List<ReleaseInfo> releases = new ArrayList<>();
		File fld = new File(filesLocation, RELEASES_FOLDER);
		if (fld.exists()) {
			for (File f : fld.listFiles()) {
				releases.add(new ReleaseInfo(f));
			}
		}
		Collections.sort(releases, new Comparator<ReleaseInfo>() {

			@Override
			public int compare(ReleaseInfo o1, ReleaseInfo o2) {
				return -Long.compare(o1.timestamp, o2.timestamp);
			}
		});
		model.addAttribute("releases", releases);
		return "admin/releases";
	}
	
	@GetMapping(path = {"/download-release"})
	public ResponseEntity<FileSystemResource> releaseDownload(String file) {
		if (file == null || file.contains("/")) {
			return ResponseEntity.badRequest().build();
		}
		File fl = new File(new File(filesLocation, RELEASES_FOLDER), file) ;
		HttpHeaders headers = new HttpHeaders();
        // headers.add(HttpHeaders.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", fl.getName()));
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + fl.getName());
		headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-binary");
		headers.add(HttpHeaders.CONTENT_LENGTH, fl.length() + "");
		return  ResponseEntity.ok().headers(headers).body(new FileSystemResource(fl));
	}
	
}
