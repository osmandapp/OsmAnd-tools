package net.osmand.server.controllers.pub;

import com.google.gson.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.transaction.Transactional;
import net.osmand.purchases.ReceiptValidationHelper;
import net.osmand.purchases.ReceiptValidationHelper.InAppReceipt;
import net.osmand.server.api.repo.*;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscriptionPrimaryKey;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.api.services.LotteryPlayService;
import net.osmand.server.api.services.ReceiptValidationService;
import net.osmand.server.utils.BTCAddrValidator;
import net.osmand.util.Algorithms;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.net.URLEncoder;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import static net.osmand.server.api.repo.DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchase;
import static net.osmand.server.api.repo.DeviceInAppPurchasesRepository.SupporterDeviceInAppPurchasePrimaryKey;
import static net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import static net.osmand.server.api.services.ReceiptValidationService.NO_SUBSCRIPTIONS_FOUND_STATUS;

@RestController
@RequestMapping("/subscription")
public class SubscriptionController {
    private static final Log LOG = LogFactory.getLog(SubscriptionController.class);

    private static final int TIMEOUT = 20000;
    public static final String PURCHASE_TYPE_SUBSCRIPTION = "subscription";
    public static final String PURCHASE_TYPE_INAPP = "inapp";
    public static final String PLATFORM_GOOGLE = "google";
    public static final String PLATFORM_APPLE = "apple";
    public static final String PLATFORM_AMAZON = "amazon";
    public static final String PLATFORM_HUAWEI = "huawei";
	public static final String PLATFORM_FASTSPRING = "fastspring";

	private static final String OSMAND_PLUS_APP = "OSMAND_PLUS_APP";

    private PrivateKey subscriptionPrivateKey;

    @Autowired
    private CloudUserDevicesRepository userDevicesRepository;

    @Autowired
    private SupportersRepository supportersRepository;

    @Autowired
    private LotteryPlayService lotteryPlayService;

    @Autowired
    private DeviceSubscriptionsRepository subscriptionsRepository;

    @Autowired
    private DeviceInAppPurchasesRepository iapsRepository;

    @Autowired
    private CloudUsersRepository usersRepository; // Keep for potentially updating User.orderId

    @Autowired
    private ReceiptValidationService validationService;

    private Gson gson = new Gson();

    private final RestTemplate restTemplate;

	@Value("${logging.purchase.debug}")
	private boolean purchaseDebugInfo;

    @Autowired
    public SubscriptionController(RestTemplateBuilder builder) {
        this.restTemplate = builder.setConnectTimeout(Duration.ofMillis(TIMEOUT)).setReadTimeout(Duration.ofMillis(TIMEOUT)).build();
        String iosSubscriptionKey = System.getenv().get("IOS_SUBSCRIPTION_KEY");
		if (!Algorithms.isEmpty(iosSubscriptionKey)) {
			byte[] pkcs8EncodedKey = Base64.getDecoder().decode(System.getenv().get("IOS_SUBSCRIPTION_KEY"));
			try {
				KeyFactory factory = KeyFactory.getInstance("EC");
				subscriptionPrivateKey = factory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8EncodedKey));
			} catch (NoSuchAlgorithmException e) {
				LOG.error(e.getMessage(), e);
			} catch (InvalidKeySpecException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

    private String encodeCredentialsToBase64(String userName, String password) {
        Base64.Encoder encoder = Base64.getMimeEncoder();
        byte[] credentials = userName.concat(":").concat(password).getBytes();
        return encoder.encodeToString(credentials);
    }

    private String processOsmUsername(String userName) {
        int ind = userName.indexOf('\'');
        if (ind > -1) {
            return userName.substring(0, ind).substring(ind + 1);
        }
        return userName;
    }

    private HttpHeaders buildHeaders(String credentials) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.set(HttpHeaders.AUTHORIZATION, String.format("Basic %s", credentials));
        return headers;
    }

    private void authenticateUser(String credentials) {
        HttpHeaders headers = buildHeaders(credentials);
        makeRequest(headers);
    }

    private void makeRequest(HttpHeaders headers) {
        HttpEntity<String> requestEntity = new HttpEntity<>(null, headers);
        restTemplate.exchange("https://api.openstreetmap.org/api/0.6/user/details",
                HttpMethod.GET, requestEntity, String.class);
    }

    private ResponseEntity<String> ok(String body, Object... args) {
		return ResponseEntity.ok().body(String.format(body, args));
	}

    private ResponseEntity<String> error(String txt) {
    	// clients don't accept error requests (neither mobile, neither http)
    	return ResponseEntity.badRequest().body(String.format("{\"error\": \"%s.\"}", txt.replace('"', '\'')));
	}

    @PostMapping(path = {"/register_email", "/register_email.php"},
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> registerEmail(HttpServletRequest request) {

		return ok(gson.toJson(lotteryPlayService.subscribeToGiveaways(request, request.getParameter("aid"),
				request.getParameter("email"), request.getParameter("os"))));
    }

	private String userInfoAsJson(Supporter s) {
		String response = String.format(
				"{\"userid\": \"%d\", \"token\": \"%s\", \"visibleName\": \"%s\", \"email\": \"%s\", "
						+ "\"preferredCountry\": \"%s\"}",
				s.userId, s.token, s.visibleName, s.userEmail, s.preferredRegion);
		return response;
	}

	protected Map<String, String> userInfoAsMap(Supporter s) {
    	Map<String, String> res = new HashMap<>();
    	res.put("userid", "" + s.userId);
		res.put("token", s.token);
		res.put("visibleName", s.visibleName);
		res.put("email", s.userEmail);
		res.put("preferredCountry", s.preferredRegion);
		return res;
	}


	@PostMapping(path = {"/register", "/register.php"}, consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE, produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> register(HttpServletRequest request) {
        String email = request.getParameter("email");
        if (email != null) {
            email = email.toLowerCase().trim();
        }
        String visibleName = request.getParameter("visibleName");
        String preferredCountry = request.getParameter("preferredCountry");
        // no email validation cause client doesn't provide it 99%
        // avoid all nulls, empty, none
        boolean emailValid = email != null && email.contains("@");
		if (emailValid) {
			Optional<Supporter> optionalSupporter = supportersRepository.findByUserEmail(email);
			if (optionalSupporter.isPresent()) {
				Supporter supporter = optionalSupporter.get();
				return ResponseEntity.ok(userInfoAsJson(supporter));
			}
		}
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int token = tlr.nextInt(100000, 1000000);
        Supporter supporter = new Supporter();
        supporter.userId = null;
        supporter.os = request.getParameter("os");
        supporter.token = String.valueOf(token);
        supporter.visibleName = visibleName;
        supporter.userEmail = email;
        supporter.preferredRegion = preferredCountry;
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(userInfoAsJson(supporter));
    }

    @PostMapping(path = {"/update", "/update.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> update(HttpServletRequest request) {
    	String userId = request.getParameter("userid");
    	if (isEmpty(userId)) {
        	return error("Please validate user id.");
        }
    	String token = request.getParameter("token");
    	if (isEmpty(token)) {
        	return error("Token is not present: fix will be in OsmAnd 3.2");
        }
        Optional<Supporter> sup = supportersRepository.findById(Long.parseLong(userId));
        if(!sup.isPresent() ) {
        	return error("Couldn't find your user id: " + userId);
        }
        Supporter supporter = sup.get();
        if(!token.equals(supporter.token)) {
        	return error("Couldn't validate the token: " + token);
        }
        supporter.visibleName = request.getParameter("visibleName");
        supporter.userEmail = request.getParameter("email");
        supporter.preferredRegion = request.getParameter("preferredCountry");
        supporter = supportersRepository.save(supporter);
        return ResponseEntity.ok(userInfoAsJson(supporter));
    }

    @PostMapping(path = {"/register_osm", "/register_osm.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> registerOsm(HttpServletRequest request) {
        String bitcoinAddress = request.getParameter("bitcoin_addr");
        // empty means deleted (keep it)
        if (bitcoinAddress != null && bitcoinAddress.length() > 0 &&
        		!BTCAddrValidator.validate(bitcoinAddress) ) {
        	return error("Please validate bitcoin address.");
        }
        String osmUser = request.getParameter("osm_usr");
        if (isEmpty(osmUser)) {
        	return error("Please validate osm user.");
        }
        String osmPassword = request.getParameter("osm_pwd");
        if (isEmpty(osmPassword)) {
        	return error("Please validate osm pwd.");
        }
        String email = request.getParameter("email");
        if (isEmpty(email) || !email.contains("@")) {
        	return error("Please validate your email address.");
        }
        String username = processOsmUsername(osmUser);
        String credentials = encodeCredentialsToBase64(username, osmPassword);
        try {
            authenticateUser(credentials);
        } catch (Exception ex) {
            return error("Please validate osm user/pwd (couldn't authenticate): " + ex.getMessage());
        }
        throw new UnsupportedOperationException();
//        OsmRecipient recipient = new OsmRecipient();
//        recipient.osmId = osmUser;
//        recipient.email = email;
//        recipient.bitcoinAddress = bitcoinAddress;
//        recipient.updateTime = new Date();
//        recipient = osmRecipientsRepository.save(recipient);
//        String response = String.format("{\"osm_user\": \"%s\", \"bitcoin_addr\": \"%s\", \"time\": \"%s\"}",
//                recipient.osmId, recipient.bitcoinAddress, recipient.updateTime.getTime()+"");
    }

    private boolean isEmpty(String s) {
		return s == null || s.length() == 0;
	}

	@PostMapping(path = { "/ios-receipt-validate" })
	public ResponseEntity<String> validateReceiptIos(HttpServletRequest request) throws Exception {
		String receipt = request.getParameter("receipt");
		JsonObject receiptObj = validationService.loadReceiptJsonObject(receipt, false);
		if (receiptObj != null) {
			Map<String, Object> result = new HashMap<>();
			List<InAppReceipt> inAppReceipts = validationService.loadInAppReceipts(receiptObj);
			if (inAppReceipts != null) {
				if (inAppReceipts.size() == 0) {
					result.put("eligible_for_introductory_price", "true");
					result.put("eligible_for_subscription_offer", "false");
					result.put("result", false);
					result.put("status", NO_SUBSCRIPTIONS_FOUND_STATUS);
				} else {
					result.put("eligible_for_introductory_price",
							isEligibleForIntroductoryPrice(inAppReceipts) ? "true" : "false");

					// update existing subscription purchaseToken
					for (InAppReceipt r : inAppReceipts) {
						if (r.isSubscription()) {
							Optional<SupporterDeviceSubscription> subscription = subscriptionsRepository.findById(
									new SupporterDeviceSubscriptionPrimaryKey(r.getProductId(), r.getOrderId()));
							if (subscription.isPresent()) {
								SupporterDeviceSubscription s = subscription.get();
								if (!Algorithms.objectEquals(s.purchaseToken, receipt)) {
									s.purchaseToken = receipt;
									subscriptionsRepository.saveAndFlush(s);
								}
							}
						}
					}

					List<Map<String, String>> activeSubscriptions = new ArrayList<>();
					Map<String, Object> validationResult = validationService.validateReceipt(receiptObj, activeSubscriptions);
					result.putAll(validationResult);
					result.put("eligible_for_subscription_offer",
							isEligibleForSubscriptionOffer(inAppReceipts, activeSubscriptions) ? "true" : "false");
				}
			}
			if (purchaseDebugInfo) {
				String ipAddress = request.getRemoteAddr();
				Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
				if (hs != null && hs.hasMoreElements()) {
					ipAddress = hs.nextElement();
				}
				LOG.info(String.format("IOS RECEIPT for %s: %s", ipAddress, receipt));
				LOG.info(gson.toJson(result));
			}
			return ResponseEntity.ok(gson.toJson(result));
		}
		return error("Cannot load receipt.");
	}


	private boolean isEligibleForIntroductoryPrice(@NonNull Collection<InAppReceipt> inAppReceipts) {
		for (InAppReceipt receipt : inAppReceipts) {
			String isTrialPeriod = receipt.fields.get("is_trial_period");
			String isInIntroOfferPeriod = receipt.fields.get("is_in_intro_offer_period");
			if ("true".equals(isTrialPeriod) || "true".equals(isInIntroOfferPeriod)) {
				return false;
			}
		}
		return true;
	}

	private boolean isEligibleForSubscriptionOffer(@NonNull Collection<InAppReceipt> inAppReceipts, @NonNull List<Map<String, String>> activeSubscriptions) {
    	if (activeSubscriptions.size() == 0) {
			for (InAppReceipt receipt : inAppReceipts) {
				if (receipt.isSubscription()) {
					return true;
				}
			}
		}
    	return false;
	}

	@PostMapping(path = { "/ios-fetch-signatures" })
	public ResponseEntity<String> fetchSignaturesIos(HttpServletRequest request) throws Exception {
		String productIdentifiers = request.getParameter("productIdentifiers");
		if (!Algorithms.isEmpty(productIdentifiers)) {
			String userId = request.getParameter("userId");
			HashMap<String, Object> result = new HashMap<>();
			List<Map<String, String>> signatures = new ArrayList<>();
			String[] productIdentifiersArray = productIdentifiers.split(";");
			for (String productIdentifier : productIdentifiersArray) {
				String discountIdentifiers = request.getParameter(productIdentifier + "_discounts");
				if (!Algorithms.isEmpty(discountIdentifiers)) {
					String[] discountIdentifiersArray = discountIdentifiers.split(";");
					for (String discountIdentifier : discountIdentifiersArray) {
						Map<String, String> signatureObj = generateOfferSignature(productIdentifier, discountIdentifier, userId);
						if (signatureObj != null) {
							signatures.add(signatureObj);
						}
					}
				}
			}
			result.put("signatures", signatures);
			result.put("status", 0);
			return ResponseEntity.ok(gson.toJson(result));
		}
		return error("Product identifiers are not defined.");
	}

	@Nullable
	private Map<String, String> generateOfferSignature(String productIdentifier, String offerIdentifier, String userId) {
    	String appBundleId = ReceiptValidationHelper.IOS_MAPS_BUNDLE_ID;
		String keyIdentifier = System.getenv().get("IOS_SUBSCRIPTION_KEY_ID");
		String nonce = UUID.randomUUID().toString().toLowerCase();
		String timestamp = "" + System.currentTimeMillis() / 1000L;
		if (Algorithms.isEmpty(userId)) {
			userId = "00000";
		}
		String usernameHash = DigestUtils.md5Hex(userId);

		String source = appBundleId + '\u2063' + keyIdentifier + '\u2063' + productIdentifier + '\u2063' + offerIdentifier + '\u2063' + usernameHash + '\u2063' + nonce + '\u2063' + timestamp;
		String signatureBase64 = null;
		if (subscriptionPrivateKey != null) {
			try {
				Signature ecdsa = Signature.getInstance("SHA256withECDSA");
				ecdsa.initSign(subscriptionPrivateKey);
				ecdsa.update(source.getBytes("UTF-8"));
				byte[] signature = ecdsa.sign();
				signatureBase64 = URLEncoder.encode(Base64.getEncoder().encodeToString(signature), "UTF-8");
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
			}
		} else {
			LOG.error("Subscription private key is null " +
					"(generateOfferSignature" +
					" productIdentifier=" + productIdentifier +
					" offerIdentifier=" + offerIdentifier +
					" usernameHash=" + usernameHash + ")");
		}
		if (signatureBase64 != null) {
			Map<String, String> result = new HashMap<>();
			result.put("appBundleId", appBundleId);
			result.put("keyIdentifier", keyIdentifier);
			result.put("productIdentifier", productIdentifier);
			result.put("offerIdentifier", offerIdentifier);
			result.put("userId", userId);
			result.put("usernameHash", usernameHash);
			result.put("nonce", nonce);
			result.put("timestamp", timestamp);
			result.put("signature", signatureBase64);
			return result;
		}
		return null;
	}


	@PostMapping(path = {"/restore-purchased"})
	public ResponseEntity<String> restorePurchased(HttpServletRequest request) {
        // Assume restore only applies to subscriptions for now, but could be extended
        return purchased(request, PURCHASE_TYPE_SUBSCRIPTION);
	}

	// Android sends
	//	parameters.put("userid", userId);
	//	parameters.put("sku", info.getSku());
	//	parameters.put("orderId", info.getOrderId());
	//	parameters.put("purchaseToken", info.getPurchaseToken());
	//	parameters.put("email", email);
	//	parameters.put("token", token);
	//	parameters.put("version", Version.getFullVersion(ctx));
	//	parameters.put("lang", ctx.getLanguage() + "");
	//	parameters.put("nd", ctx.getAppInitializer().getFirstInstalledDays() + "");
	//	parameters.put("ns", ctx.getAppInitializer().getNumberOfStarts() + "");
	//	parameters.put("aid", ctx.getUserAndroidId());
	// iOS sends
    // [params setObject:@"ios" forKey:@"os"];
    // [params setObject:userId forKey:@"userid"];
    // [params setObject:token forKey:@"token"];
    // [params setObject:sku forKey:@"sku"];
    // [params setObject:transactionId forKey:@"purchaseToken"];
    // [params setObject:receiptStr forKey:@"payload"];
    // [params setObject:email forKey:@"email"];
	@Transactional
    @PostMapping(path = {"/purchased", "/purchased.php"})
    public ResponseEntity<String> purchased(HttpServletRequest request,
                                            @RequestParam(name = "purchaseType", required = false, defaultValue = PURCHASE_TYPE_SUBSCRIPTION) String purchaseType
    ) {

        StringBuilder req = new StringBuilder("Purchase info: ");
        for (Entry<String, String[]> s : request.getParameterMap().entrySet()) {
            req.append(s.getKey()).append("=").append(Arrays.toString(s.getValue())).append(" ");
        }
        LOG.info(req);

        // --- Identify Credentials Provided ---
        String userDeivceIdParam = request.getParameter("deviceid"); // UserDevice ID
        String userDeivceAccessTokenParam = request.getParameter("accessToken");   // UserDevice Access Token
        String supporterUserIdParam = request.getParameter("userid"); // Supporter ID
        String supporterTokenParam = request.getParameter("token"); // Supporter Token

        // Purchase details
        String orderIdParam = request.getParameter("orderId");
        String purchaseTokenParam = request.getParameter("purchaseToken");
        String payloadParam = request.getParameter("payload");
        String skuParam = request.getParameter("sku");

        // Platform and Effective IDs
        String platformParam = request.getParameter("platform");
        if (!Algorithms.isEmpty(platformParam)) {
            platformParam = platformParam.toLowerCase().trim();
        }
        boolean ios = PLATFORM_APPLE.equals(platformParam) || Algorithms.isEmpty(orderIdParam);
        String platform = ios ? PLATFORM_APPLE : platformParam;
        String effectivePurchaseToken = ios ? payloadParam : purchaseTokenParam;
        String effectiveOrderId = ios ? purchaseTokenParam : orderIdParam;

        // Validation of purchase details
        if (isEmpty(effectiveOrderId) || isEmpty(effectivePurchaseToken) || isEmpty(skuParam)) {
            return error("Purchase details (orderId/transactionId, purchaseToken/payload, sku) are incomplete.");
        }

        Integer userId = resolveUserId(request, userDeivceIdParam, userDeivceAccessTokenParam);
        Long supporterUserId = resolveSupporterId(request, supporterUserIdParam, supporterTokenParam, effectiveOrderId,
                PURCHASE_TYPE_SUBSCRIPTION.equalsIgnoreCase(purchaseType));

        // --- Process Purchase ---
        if (PURCHASE_TYPE_SUBSCRIPTION.equalsIgnoreCase(purchaseType)) {
            SupporterDeviceSubscription subscr = new SupporterDeviceSubscription();
            subscr.purchaseToken = effectivePurchaseToken;
            subscr.orderId = effectiveOrderId;
            subscr.sku = skuParam;
            subscr.userId = userId;
            subscr.supporterId = supporterUserId;
            subscr.timestamp = new Date(); // Record creation/update time

	        updateUserOrderId(subscr.userId, subscr.orderId);

            Optional<SupporterDeviceSubscription> subscrOpt = subscriptionsRepository.findById(
                    new SupporterDeviceSubscriptionPrimaryKey(subscr.sku, subscr.orderId));
            if (subscrOpt.isPresent()) { // Update existing subscription record
                SupporterDeviceSubscription dbSubscription = subscrOpt.get();
                // Only update if purchaseToken changes (e.g., iOS receipt update)
                // Or if it was previously null (though our validation above tries to prevent this)
                if (!Algorithms.isEmpty(subscr.purchaseToken) && !Algorithms.objectEquals(subscr.purchaseToken, dbSubscription.purchaseToken)) {
                    if (dbSubscription.valid != null && dbSubscription.valid) {
                        dbSubscription.prevvalidpurchasetoken = dbSubscription.purchaseToken;
                    }
                    // Reset validation status when token changes, let verification task re-check
                    dbSubscription.valid = null;
                    dbSubscription.kind = null;
                    dbSubscription.purchaseToken = subscr.purchaseToken;
	                if (subscr.userId != null) {
		                dbSubscription.userId = subscr.userId;
	                }
                    if (subscr.supporterId != null) {
                        dbSubscription.supporterId = subscr.supporterId;
                    }
                    dbSubscription.timestamp = subscr.timestamp; // Update timestamp
                    subscriptionsRepository.save(dbSubscription);
                    LOG.info("Updated existing subscription: sku=" + subscr.sku + ", orderId=" + subscr.orderId);
                } else {
	                if (dbSubscription.userId == null && subscr.userId != null) {
		                dbSubscription.userId = subscr.userId;
		                dbSubscription.timestamp = subscr.timestamp;
		                subscriptionsRepository.save(dbSubscription);
		                LOG.info("Updated existing subscription: sku=" + subscr.sku + ", orderId=" + subscr.orderId);
	                } else {
		                LOG.info("Subscription already exists, no changes needed: sku=" + subscr.sku + ", orderId=" + subscr.orderId);
	                }
                }
            } else { // Insert new subscription record
                subscriptionsRepository.save(subscr);
                LOG.info("Saved new subscription: sku=" + subscr.sku + ", orderId=" + subscr.orderId);
            }
            return ResponseEntity.ok("{ \"res\" : \"OK\", \"type\": \"subscription\" }");

        } else if (PURCHASE_TYPE_INAPP.equalsIgnoreCase(purchaseType)) {
	        ResponseEntity<String> error = processOsmandPlusAppPurchase(userId, effectiveOrderId, skuParam);
			if (error != null) {
				return error;
			}
            SupporterDeviceInAppPurchase iap = new SupporterDeviceInAppPurchase();
            iap.purchaseToken = effectivePurchaseToken;
            iap.orderId = effectiveOrderId; // Google orderId or Apple transaction_id
            iap.sku = skuParam;
            iap.platform = platform;
            iap.valid = null; // Needs verification
            iap.timestamp = new Date(); // Record creation time
            iap.userId = userId;
            iap.supporterId = supporterUserId;

            Optional<SupporterDeviceInAppPurchase> iapOpt = iapsRepository.findById(
                    new SupporterDeviceInAppPurchasePrimaryKey(iap.sku, iap.orderId));
            if (iapOpt.isPresent()) {
                SupporterDeviceInAppPurchase dbIap = iapOpt.get();
                // Update only if userId was null and is now being set, or if context changed
                if ((dbIap.userId == null && iap.userId != null) || (dbIap.supporterId == null && iap.supporterId != null)) {
                    if (iap.userId != null) {
                        dbIap.userId = iap.userId;
                    }
                    if (iap.supporterId != null) {
                        dbIap.supporterId = iap.supporterId;
                    }
                    dbIap.timestamp = iap.timestamp; // Update timestamp on link change
                    iapsRepository.save(dbIap);
                    LOG.info("Updated/Linked existing IAP: sku=" + iap.sku + ", orderId=" + iap.orderId + " to userId " + iap.userId + " to supporterId " + iap.supporterId + ")");
                } else {
                    LOG.info("IAP already exists/linked, no changes needed: sku=" + iap.sku + ", orderId=" + iap.orderId);
                }
            } else { // Insert new IAP record
                iapsRepository.save(iap);
                LOG.info("Saved new IAP: sku=" + iap.sku + ", orderId=" + iap.orderId + " with userId " + iap.userId + " with supporterId " + iap.supporterId + ")");
            }
            return ResponseEntity.ok("{ \"res\" : \"OK\", \"type\": \"inapp\" }");
        } else {
            return error("Invalid purchaseType specified: " + purchaseType);
        }
    }

	private ResponseEntity<String> processOsmandPlusAppPurchase(Integer userId, String orderId, String sku) {
		if (!Algorithms.isEmpty(orderId) && orderId.equals(OSMAND_PLUS_APP) && !Algorithms.isEmpty(sku) && sku.equals("osmand_full_version_price")) {
			if (userId == null) {
				return error("User ID is not provided for OsmAnd+ App purchase.");
			}
			CloudUsersRepository.CloudUser pu = usersRepository.findById(userId);
			if (pu == null) {
				return error("User not found. To purchase Maps+ in-app features, please register first.");
			}
			return null;
		}
		return null;
	}

	private void updateUserOrderId(Integer userId, String orderId) {
		if (userId != null) {
			CloudUsersRepository.CloudUser pu = usersRepository.findById(userId);
			if (pu != null && pu.orderid == null) {
				pu.orderid = orderId;
				usersRepository.saveAndFlush(pu);
			}
		}
	}

    private Integer resolveUserId(HttpServletRequest request, String userDeviceIdParam,
                                         String userDeviceAccessTokenParam) {
        if (!isEmpty(userDeviceIdParam) && !isEmpty(userDeviceAccessTokenParam)) {
            try {
                int userDeviceId = Integer.parseInt(userDeviceIdParam);
                CloudUserDevice userDevice = userDevicesRepository.findById(userDeviceId);
                if (userDevice != null && userDeviceAccessTokenParam.equals(userDevice.accesstoken)) {
                    return userDevice.userid;
                } else {
                    LOG.warn("UserDevice access token mismatch or device not found: " + toString(request.getParameterMap()));
                }
            } catch (NumberFormatException e) {
                LOG.error("Invalid userId format in /purchased: " + userDeviceIdParam);
            }
        }
        return null;
    }

    private Long resolveSupporterId(HttpServletRequest request, String supporterUserIdParam,
                                    String supporterTokenParam, String orderId, boolean updateOrderId) {
        Long res = null;
        if (!isEmpty(supporterUserIdParam) && !isEmpty(supporterTokenParam)) {
            try {
                long supporterId = Long.parseLong(supporterUserIdParam);
                Optional<Supporter> supOpt = supportersRepository.findById(supporterId);
                if (supOpt.isPresent() && supporterTokenParam.equals(supOpt.get().token)) {
                    res = supporterId;
                    // Link subscription orderId to Supporter if needed (handle conflicts)
                    if (updateOrderId) {
                        Supporter supporter = supOpt.get();
                        if (!Algorithms.objectEquals(supporter.orderId, orderId)) {
                            supporter.orderId = orderId;
                            supportersRepository.saveAndFlush(supporter);
                        }
                    }
                } else {
                    LOG.warn("Supporter token mismatch or supporter not found: " + toString(request.getParameterMap()));
                }
            } catch (NumberFormatException | ArithmeticException e) {
                LOG.warn("Invalid supporterUserId format in /purchased: " + supporterUserIdParam);
            }
        }
        return res;
    }

	private String toString(Map<String, String[]> parameterMap) {
		StringBuilder bld = new StringBuilder();
		for(String s : parameterMap.keySet()) {
			bld.append(" ").append(s).append("=").append(Arrays.toString(parameterMap.get(s)));
		}
		return bld.toString();
	}
}
