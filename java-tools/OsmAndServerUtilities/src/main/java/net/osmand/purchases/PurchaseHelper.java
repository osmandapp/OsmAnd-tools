package net.osmand.purchases;

public class PurchaseHelper {

	public static final String PLATFORM_GOOGLE = "google";
	public static final String PLATFORM_APPLE = "apple";
	public static final String PLATFORM_AMAZON = "amazon";
	public static final String PLATFORM_HUAWEI = "huawei";
	public static final String PLATFORM_FASTSPRING = "fastspring";

	public static final String PLATFORM_KEY = "platform";

	public static String getPlatformBySku(String sku) {
		if (sku == null || sku.isEmpty()) {
			return null;
		}
		if (sku.startsWith("osm_live_subscription_")
				|| sku.startsWith("osm_free_live_subscription_")
				|| sku.startsWith("osmand_pro_monthly_")
				|| sku.startsWith("osmand_pro_annual_")
				|| sku.startsWith("osmand_maps_annual_")
				|| sku.startsWith("osmand_full_version_price")
				|| sku.startsWith("net.osmand.contourlines")
				|| sku.startsWith("net.osmand.seadepth")) {
			return PLATFORM_GOOGLE;
		}
		if (sku.startsWith("net.osmand.maps.")) {
			return PLATFORM_APPLE;
		}
		if (sku.contains(".huawei.")) {
			return PLATFORM_HUAWEI;
		}
		if (sku.contains(".amazon.")) {
			return PLATFORM_AMAZON;
		}
		if (sku.contains(".fastspring.")) {
			return PLATFORM_FASTSPRING;
		}
		return null;
	}
}
