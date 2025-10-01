package net.osmand.obf.preparation;

import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gnu.trove.set.hash.TLongHashSet;
import net.osmand.obf.preparation.OsmDbAccessor.OsmDbTagsPreparation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.util.Algorithms;

public class MissingWikiTagsProcessor implements OsmDbTagsPreparation {
	private static final Log log = LogFactory.getLog(MissingWikiTagsProcessor.class);
	private final String wikidataMappingUrl;
	private final String wikirankingUrl;
	Boolean init = null;
	private PreparedStatement selectById;
	private PreparedStatement selectId;
	private PreparedStatement selectRankById;
	private TLongHashSet assignedWids = new TLongHashSet();

	public MissingWikiTagsProcessor(String wikidataMappingUrl, String wikirankingUrl) {
		this.wikidataMappingUrl = wikidataMappingUrl;
		this.wikirankingUrl = wikirankingUrl;
	}

	
	@Override
	public void processTags(Entity e) {
		String wikidata = e.getTag(OSMTagKey.WIKIDATA);
		String wikipedia = e.getTag(OSMTagKey.WIKIPEDIA);
		if (wikipedia == null && wikidata == null) {
			return;
		}	
		syncReadTags(e, wikidata, wikipedia);
	}

	private synchronized void syncReadTags(Entity e, String wikidata, String wikipedia) {
		try {
			if (!init()) {
				return;
			}
			if (wikipedia == null && wikidata == null) {
				return;
			}
			long wikidataId;
			if (wikidata != null) {
				String ind = wikidata.substring(wikidata.lastIndexOf('Q') + 1);
				wikidataId = Algorithms.parseLongSilently(ind, 0);
				if (wikidataId != 0 && wikipedia == null) {
					assignWikipediaTags(e, wikidataId);
				}
			} else {
				wikidataId = findAndAssignWikidataId(e, wikipedia);
			}
			// assign wiki place
			// wikidata != null  filter duplicates
			if (wikidata != null && wikidataId != 0 && selectRankById != null) {
				boolean added = assignedWids.add(wikidataId);
				if (added) {
					assignRankingTags(e, wikidataId);
				}
			}
		} catch (Exception es) {
			log.error(es.getMessage(), es);
		}
	}


	private void assignRankingTags(Entity e, long wikidataId) throws SQLException {
		selectRankById.setLong(1, wikidataId);
		ResultSet rankIdRes = selectRankById.executeQuery();
		if (rankIdRes.next()) {
			double travelElo = rankIdRes.getDouble("elo");
			int qrank = rankIdRes.getInt("qrank");
			int travelTopic = rankIdRes.getInt("topic");
			String photoTitle = rankIdRes.getString("photoTitle");
			String catTitle = rankIdRes.getString("catTitle");
			String poiKey = rankIdRes.getString("poikey");
			if (travelElo > 0) {
				e.putTag("osmwiki", "wiki_place");
				e.putTag("travel_elo", "" + (int) travelElo);
			}
			if (qrank > 0) {
				e.putTag("qrank", "" + qrank);
			}
			if (travelTopic > 0) {
				e.putTag("travel_topic", "" + travelTopic);
			}
			if (!Algorithms.isEmpty(photoTitle)) {
				e.putTag("wiki_photo", "" + photoTitle);
			}
			if (!Algorithms.isEmpty(catTitle)) {
				e.putTag("wiki_category", "" + catTitle);
			}
			if (!Algorithms.isEmpty(poiKey)) {
				e.putTag("osmand_poi_key", "" + poiKey);
			}
		}
		rankIdRes.close();
	}


	private long findAndAssignWikidataId(Entity e, String wikipedia) throws SQLException {
		// wikipedia != null && wikidata == null!
		int langInd = wikipedia.indexOf(':');
		String lang = "en";
		String title = wikipedia;
		if (langInd >= 0) {
			lang = wikipedia.substring(0, langInd);
			title = wikipedia.substring(langInd + 1);
		}
		selectId.setString(1, lang);
		selectId.setString(2, title);
		ResultSet rs = selectId.executeQuery();
		long wikidataId = 0;
		if (rs.next()) {
			wikidataId = rs.getLong(1);
			e.putTag("wikidata", "Q" + wikidataId);
		}
		rs.close();
		return wikidataId;
	}


	private void assignWikipediaTags(Entity e, long wikidataId) throws SQLException {
		selectById.setLong(1, wikidataId);
		ResultSet rs = selectById.executeQuery();
		try {
			String lang = null, title = null;
			int cntLangs = 0;
			while (rs.next()) {
				lang = rs.getString(1);
				title = rs.getString(2);
				cntLangs++;
				if (lang == null) {
					System.out.println("Null language for " + title + " Q" + wikidataId);
				}
				if (lang.length() == 2) { // for main languages length = 2 (wikivoyage > 2)
					if (lang.equals("en")) {
						e.putTag("wikipedia", "en:" + title);
					} else {
						// ignore 2nd languages as it's too much data and not by OSM standard
//							e.putTag("wikipedia:" + lang, title);
					}
				}
			}
			if (cntLangs == 1 && !lang.equals("en")) {
				e.removeTag("wikipedia:" + lang);
				e.putTag("wikipedia", lang + ":" + title);
			}
		} finally {
			rs.close();
		}
	}

	private boolean init() {
		if (init != null) {
			return init.booleanValue();
		}
		try {
			init = false;
			String fileName = wikidataMappingUrl;
			
			if (wikidataMappingUrl.startsWith("http")) {
				log.info("Downloading wikidata database by url " + wikidataMappingUrl);
				URL url = new URL(wikidataMappingUrl);
				URLConnection cn = url.openConnection();
				fileName = "wikidata_mapping.sqlitedb";
				FileOutputStream fous = new FileOutputStream(fileName);
				Algorithms.streamCopy(cn.getInputStream(), fous);
				fous.close();
				log.info("Finished downloading wikidata database");
			} else {
				log.info("Using local wikidata database: " + wikidataMappingUrl);
			}
			
			
			Connection wikiMapping = DBDialect.SQLITE.getDatabaseConnection(fileName, log);
			selectById = wikiMapping.prepareStatement("SELECT lang, title from wiki_mapping where id = ?");
			selectId = wikiMapping.prepareStatement("SELECT id from wiki_mapping where lang = ? and title = ? ");
			
			if (!Algorithms.isEmpty(wikirankingUrl)) {
				String wikiRanking = wikirankingUrl;
				if (wikirankingUrl.startsWith("http")) {
					log.info("Downloading wikiranking database by url " + wikirankingUrl);
					URL url = new URL(wikirankingUrl);
					URLConnection cn = url.openConnection();
					wikiRanking = "wiki_ranking.sqlitedb";
					FileOutputStream fous = new FileOutputStream(wikiRanking);
					Algorithms.streamCopy(cn.getInputStream(), fous);
					fous.close();
					log.info("Finished downloading wikiranking database");
				} else {
					log.info("Using local wikiranking database: " + wikirankingUrl);
				}
				Connection wikiRankingConn = DBDialect.SQLITE.getDatabaseConnection(wikiRanking, log);
				selectRankById = wikiRankingConn.prepareStatement("SELECT photoId, photoTitle, catId, catTitle, poikey, "
						+ "wikiTitle, osmid, osmtype, elo, qrank, topic, categories FROM wiki_rating WHERE id = ?");
			}
			init = true;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return init.booleanValue();
	}

}
