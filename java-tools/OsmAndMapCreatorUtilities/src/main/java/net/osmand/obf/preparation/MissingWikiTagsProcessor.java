package net.osmand.obf.preparation;

import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.obf.preparation.OsmDbAccessor.OsmDbTagsPreparation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.util.Algorithms;

public class MissingWikiTagsProcessor implements OsmDbTagsPreparation {
	private static final Log log = LogFactory.getLog(MissingWikiTagsProcessor.class);
	private final String wikidataMappingUrl;
	Boolean init = null;
	private PreparedStatement selectById;
	private PreparedStatement selectId;

	public MissingWikiTagsProcessor(String wikidataMappingUrl) {
		this.wikidataMappingUrl = wikidataMappingUrl;
	}

	@Override
	public void processTags(Entity e) {
		String wikidata = e.getTag(OSMTagKey.WIKIDATA);
		String wikipedia = e.getTag(OSMTagKey.WIKIPEDIA);
		try {
			if (wikipedia != null && wikidata == null) {
				if (!init()) {
					return;
				}
				int langInd = wikipedia.indexOf(':');
				String lang = "en";
				String title = wikipedia;
				if (langInd >= 0) {
					lang = wikipedia.substring(0, langInd);
					title = wikipedia.substring(langInd + 1);
				}
				selectId.setString(1, lang);
				selectId.setString(2, title);
				ResultSet rs = selectById.executeQuery();
				if (rs.next()) {
					e.putTag("wikidata", "Q" + rs.getLong(1));
				}
			} else if (wikidata != null && wikipedia == null) {
				if (!init()) {
					return;
				}
				String ind = wikidata.substring(wikidata.lastIndexOf('Q') + 1);
				long wid = Algorithms.parseLongSilently(ind, 0);
				if (wid == 0) {
					return;
				}
				selectById.setLong(1, wid);
				ResultSet rs = selectById.executeQuery();
				String lang = null, title = null;
				int cntLangs = 0;
				while (rs.next()) {
					lang = rs.getString(1);
					title = rs.getString(2);
					cntLangs++;
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
			}
		} catch (Exception es) {
			log.error(es.getMessage(), es);
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
				URL url = new URL(wikidataMappingUrl);
				URLConnection cn = url.openConnection();
				fileName = "wikidata_mapping.sqlitedb";
				FileOutputStream fous = new FileOutputStream(fileName);
				Algorithms.streamCopy(cn.getInputStream(), fous);
				fous.close();
			}
			Connection wikiMapping = DBDialect.SQLITE.getDatabaseConnection(fileName, log);
			selectById = wikiMapping.prepareStatement("SELECT lang, title from wiki_mapping where id = ?");
			selectId = wikiMapping.prepareStatement("SELECT id from wiki_mapping where lang = ? and title = ? ");
			init = true;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return init.booleanValue();
	}

}
