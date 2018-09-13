package net.osmand.data.changeset;

public enum OsmAndLiveReportType {
	REGION_RANKING_RANGE("getRegionRankingRange", true),
	RANKING_RANGE("getRankingRange", true),
	MIN_CHANGES("getMinChanges", true),
	EUR_BTC_RATE("getBTCEurRate", true),
	BTC_VALUE("getBTCValue", true),
	EUR_VALUE("getEurValue", true),
	
	SUPPORTERS("getSupporters"),
	COUNTRIES("getCountries"),
	RECIPIENTS("getRecipients"),
	TOTAL_CHANGES("getTotalChanges"),
	USERS_RANKING("calculateUsersRanking"),
	USER_RANKING("calculateUserRanking"),
	RANKING("calculateRanking");
	
	private final String sqlName;
	private final boolean numberReport;

	OsmAndLiveReportType(String sqlName, boolean numberReport) {
		this.sqlName = sqlName;
		this.numberReport = numberReport;
	}
	
	OsmAndLiveReportType(String sqlName) {
		this(sqlName, false);
	}
	
	public boolean isNumberReport() {
		return numberReport;
	}
	
	public String getSqlName() {
		return sqlName;
	}

}
