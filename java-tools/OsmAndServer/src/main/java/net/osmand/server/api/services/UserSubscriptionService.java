package net.osmand.server.api.services;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisher.Purchases.Subscriptions;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.google.gson.JsonObject;

import net.osmand.live.subscriptions.AmazonIAPHelper;
import net.osmand.live.subscriptions.HuaweiIAPHelper;
import net.osmand.live.subscriptions.ReceiptValidationHelper;
import net.osmand.live.subscriptions.UpdateSubscription;
import net.osmand.live.subscriptions.AmazonIAPHelper.AmazonSubscription;
import net.osmand.live.subscriptions.HuaweiIAPHelper.HuaweiSubscription;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository;
import net.osmand.server.api.repo.DeviceSubscriptionsRepository.SupporterDeviceSubscription;
import net.osmand.util.Algorithms;

@Service
public class UserSubscriptionService {

	private static final String OSMAND_PRO_ANDROID_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_ANDROID_SUBSCRIPTION_PREFIX;
	private static final String OSMAND_PROMO_SUBSCRIPTION = "promo_";
	private static final String GOOGLE_PACKAGE_NAME = UpdateSubscription.GOOGLE_PACKAGE_NAME;
	private static final String GOOGLE_PACKAGE_NAME_FREE = UpdateSubscription.GOOGLE_PACKAGE_NAME_FREE;

	private static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_1 = UpdateSubscription.OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_M;
	private static final String OSMAND_PRO_HUAWEI_SUBSCRIPTION_2 = UpdateSubscription.OSMAND_PRO_HUAWEI_SUBSCRIPTION_PART_Y;
	private static final String OSMAND_PRO_AMAZON_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_AMAZON_SUBSCRIPTION_PART;
	private static final String OSMAND_PRO_IOS_SUBSCRIPTION = UpdateSubscription.OSMAND_PRO_IOS_SUBSCRIPTION_PREFIX;
	
	private static final Log LOG = LogFactory.getLog(UserSubscriptionService.class);
	
	@Value("${google.androidPublisher.clientSecret}")
	protected String clientSecretFile;
	
	@Autowired
	protected DeviceSubscriptionsRepository subscriptionsRepo;

	private AndroidPublisher androidPublisher;
	private HuaweiIAPHelper huaweiIAPHelper;
	private AmazonIAPHelper amazonIAPHelper;


	public String checkOrderIdPremium(String orderid) {
		if (Algorithms.isEmpty(orderid)) {
			return "no subscription provided";
		}
		String errorMsg = "no subscription present";
		List<SupporterDeviceSubscription> lst = subscriptionsRepo.findByOrderId(orderid);
		for (SupporterDeviceSubscription s : lst) {
			// s.sku could be checked for premium
			if (s.expiretime == null || s.expiretime.getTime() < System.currentTimeMillis() || s.checktime == null) {
				if (s.sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION)) {
					s = revalidateGoogleSubscription(s);
				} else if (s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_1) || s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_2)) {
					s = revalidateHuaweiSubscription(s);
				} else if (s.sku.contains(OSMAND_PRO_AMAZON_SUBSCRIPTION)) {
					s = revalidateAmazonSubscription(s);
				}
			}
			if (s.valid == null || !s.valid.booleanValue()) {
				errorMsg = "no valid subscription present";
			} else if (!s.sku.startsWith(OSMAND_PRO_ANDROID_SUBSCRIPTION) &&
					!s.sku.startsWith(OSMAND_PROMO_SUBSCRIPTION) &&
					!s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_1) &&
					!s.sku.contains(OSMAND_PRO_HUAWEI_SUBSCRIPTION_2) &&
					!s.sku.contains(OSMAND_PRO_AMAZON_SUBSCRIPTION)) {
				errorMsg = "subscription is not eligible for OsmAnd Cloud";
			} else {
				if (s.expiretime != null && s.expiretime.getTime() > System.currentTimeMillis()) {
					return null;
				} else {
					errorMsg = "subscription is expired or not validated yet";
				}
			}
		}
		return errorMsg;
	}
	
	private SupporterDeviceSubscription revalidateGoogleSubscription(SupporterDeviceSubscription s) {
		if (!Algorithms.isEmpty(clientSecretFile) ) {
			if (androidPublisher == null) {
				try {
					// watch first time you need to open special url on web server
					this.androidPublisher = UpdateSubscription.getPublisherApi(clientSecretFile);
				} catch (Exception e) {
					LOG.error("Error configuring android publisher api: " + e.getMessage(), e);
				}
			}
		}
		if (androidPublisher != null) {
			try {
				Subscriptions subs = androidPublisher.purchases().subscriptions();
				SubscriptionPurchase subscription;
				if (s.sku.contains("_free_")) {
					subscription = subs.get(GOOGLE_PACKAGE_NAME_FREE, s.sku, s.purchaseToken).execute();
				} else {
					subscription = subs.get(GOOGLE_PACKAGE_NAME, s.sku, s.purchaseToken).execute();
				}
				if (subscription != null) {
					if (s.expiretime == null || s.expiretime.getTime() < subscription.getExpiryTimeMillis()) {
						s.expiretime = new Date(subscription.getExpiryTimeMillis());
						// s.checktime = new Date(); // don't set checktime let jenkins do its job
						s.valid = System.currentTimeMillis() < subscription.getExpiryTimeMillis();
						subscriptionsRepo.save(s);
					}
				}
			} catch (IOException e) {
				LOG.error(String.format("Error retrieving android publisher subscription %s - %s: %s",
						s.sku, s.orderId, e.getMessage()), e);
			}
		}
		return s;
	}

	private SupporterDeviceSubscription revalidateHuaweiSubscription(SupporterDeviceSubscription s) {
		if (huaweiIAPHelper == null) {
			huaweiIAPHelper = new HuaweiIAPHelper();
		}
		try {
			if (s.orderId == null) {
				return s;
			}
			HuaweiSubscription subscription = huaweiIAPHelper.getHuaweiSubscription(s.orderId, s.purchaseToken);
			if (subscription != null) {
				if (s.expiretime == null || s.expiretime.getTime() < subscription.expirationDate) {
					s.expiretime = new Date(subscription.expirationDate);
					// s.checktime = new Date(); // don't set checktime let jenkins do its job
					s.valid = System.currentTimeMillis() < subscription.expirationDate;
					subscriptionsRepo.save(s);
				}
			}
		} catch (IOException e) {
			LOG.error(String.format("Error retrieving huawei subscription %s - %s: %s",
					s.sku, s.orderId, e.getMessage()), e);
		}
		return s;
	}

	private SupporterDeviceSubscription revalidateAmazonSubscription(SupporterDeviceSubscription s) {
		if (amazonIAPHelper == null) {
			amazonIAPHelper = new AmazonIAPHelper();
		}
		try {
			if (s.orderId == null) {
				return s;
			}
			AmazonSubscription subscription = amazonIAPHelper.getAmazonSubscription(s.orderId, s.purchaseToken);
			if (subscription != null) {
				if (s.expiretime == null || s.expiretime.getTime() < subscription.renewalDate) {
					s.expiretime = new Date(subscription.renewalDate);
					// s.checktime = new Date(); // don't set checktime let jenkins do its job
					s.valid = System.currentTimeMillis() < subscription.renewalDate;
					subscriptionsRepo.save(s);
				}
			}
		} catch (IOException e) {
			LOG.error(String.format("Error retrieving amazon subscription %s - %s: %s",
					s.sku, s.orderId, e.getMessage()), e);
		}
		return s;
	}

    public JsonObject revalidateiOSSubscription(String orderid) {
        String errorMsg = "";
        List<SupporterDeviceSubscription> lst = subscriptionsRepo.findByOrderId(orderid);
        SupporterDeviceSubscription result = null;
        for (SupporterDeviceSubscription s : lst) {
            if (!s.sku.startsWith(OSMAND_PRO_IOS_SUBSCRIPTION)) {
                errorMsg = "subscription has not iOS sku " + s.sku;
                continue;
            }
            // s.sku could be checked for premium
            if (s.expiretime == null || s.expiretime.getTime() < System.currentTimeMillis() || s.checktime == null) {
                ReceiptValidationHelper receiptValidationHelper = new ReceiptValidationHelper();
                String purchaseToken = s.purchaseToken;
                try {
                    ReceiptValidationHelper.ReceiptResult loadReceipt = receiptValidationHelper.loadReceipt(purchaseToken);
                    if (loadReceipt.result) {
                        JsonObject receiptObj = loadReceipt.response;
                        SubscriptionPurchase subscription = null;
                        subscription = UpdateSubscription.parseIosSubscription(s.sku, s.orderId, s.introcycles == null ? 0 : s.introcycles, receiptObj);
                        if (subscription != null) {
                            if (s.expiretime == null || s.expiretime.getTime() < subscription.getExpiryTimeMillis()) {
                                s.expiretime = new Date(subscription.getExpiryTimeMillis());
                                // s.checktime = new Date(); // don't set checktime let jenkins do its job
                                s.valid = System.currentTimeMillis() < subscription.getExpiryTimeMillis();
                                subscriptionsRepo.save(s);
                            }
                            result = s;
                            errorMsg = "";
                            break;
                        } else {
                            errorMsg = "no purchases purchase format";
                        }
                    } else if (loadReceipt.error == ReceiptValidationHelper.USER_GONE) {
                        errorMsg = "Apple code:" + loadReceipt.error + " The user account cannot be found or has been deleted";
                    } else if (loadReceipt.error == ReceiptValidationHelper.SANDBOX_ERROR_CODE_TEST) {
                        errorMsg = "Apple code:" + loadReceipt.error + " This receipt is from the test environment";
                    } else {
                        errorMsg = "Apple code:" + loadReceipt.error + " See code status https://developer.apple.com/documentation/appstorereceipts/status";
                    }
                } catch (RuntimeException e) {
                    errorMsg = e.getMessage();
                }
            } else {
                result = s;
                errorMsg = "";
                break;
            }
        }
        if (errorMsg.isEmpty()) {
            if (result == null || result.valid == null || !result.valid) {
                errorMsg = "no valid subscription present";
            } else if (!result.sku.startsWith(OSMAND_PRO_IOS_SUBSCRIPTION)) {
                errorMsg = "subscription has not iOS sku " + result.sku;
            } else if (result.expiretime == null || result.expiretime.getTime() < System.currentTimeMillis()) {
                errorMsg = "subscription is expired or not validated yet";
            }
        }
        JsonObject json = new JsonObject();
        if (errorMsg.isEmpty()) {
            json.addProperty("sku", result.sku);
            json.addProperty("starttime", result.starttime.getTime());
            json.addProperty("expiretime", result.expiretime.getTime());
        } else {
            json.addProperty("error", errorMsg);
        }
        return json;
    }

	

}
