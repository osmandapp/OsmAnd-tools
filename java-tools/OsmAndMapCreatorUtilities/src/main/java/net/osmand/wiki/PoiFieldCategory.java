package net.osmand.wiki;

public enum PoiFieldCategory {
	SEE("special_photo_camera", 0xCC10A37E, new String[] { "see", "voir", "veja", "מוקדי", "دیدن" }, "church", "mosque",
			"square", "town_hall", "building", "veja", "voir", "temple", "mosque", "synagogue", "monastery", "palace",
			"château", "memorial", "archaeological", "fort", "monument", "castle", "دیدن", "tower", "cathedral",
			"arts centre", "mill", "house", "ruins"),
	DO("special_photo_camera", 0xCC10A37E, new String[] { "do", "event", "פעילויות", "انجام‌دادن" }, "museum", "zoo",
			"theater", "fair", "faire", "cinema", "disco", "sauna", "aquarium", "swimming", "amusement", "golf", "club",
			"sports", "music", "spa", "انجام‌دادن", "festival"),
	EAT("restaurants", 0xCCCA2D1D, new String[] { "eat", "manger", "coma", "אוכל", "خوردن" }, "restaurant", "cafe",
			"bistro"),
	DRINK("restaurants", 0xCCCA2D1D, new String[] { "drink", "boire", "beba", "שתייה", "نوشیدن" }, "bar", "pub"),
	SLEEP("tourism_hotel", 0xCC0E53C9, new String[] { "sleep", "se loger", "durma", "לינה", "خوابیدن" }, "hotel",
			"hostel", "habitat", "campsite"),
	BUY("shop_department_store", 0xCC8F2BAB, new String[] { "buy", "קניות", "فهرست‌بندی" }, "shop", "market", "mall"),
	GO("public_transport_stop_position", 0xCC0F5FFF,
			new String[] { "go", "destination", "aller", "circuler", "sortir", "רשימה" }, "airport", "train", "station",
			"bus"),
	NATURAL("special_photo_camera", 0xCC10A37E, new String[] { "landscape", "island", "nature", "island" }, "park",
			"cemetery", "garden", "lake", "beach", "landmark", "cemetery", "cave", "garden", "waterfall", "viewpoint",
			"mountain"),
	OTHER("", 0xCC0F5FFF, new String[] { "other", "marker", "ville", "item", "רשימה", "دیدن", "יעד מרכזי",
			"יישוב מרכזי", "représentation diplomatique" });

	public final String[] names;
	public final String[] types;
	public final String icon;
	public final int color;

	private PoiFieldCategory(String icon, int color, String[] names, String... types) {
		this.icon = icon;
		this.color = color;
		this.names = names;
		this.types = types;
	}

}