package net.osmand.server;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.osmand.wiki.commonswiki.WikiLangConverter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.lang.reflect.Type;
import java.util.*;


@RunWith(SpringRunner.class)
@SpringBootTest
public class WikiLangsConverterTest {

	@Autowired
	@Qualifier("wikiJdbcTemplate")
	JdbcTemplate jdbcTemplate;

	private static final Gson gson = new Gson();
	private static final Type mapType = new TypeToken<Map<String, String>>() {
	}.getType();

	long count = 0;
	Map<String, Stat> valid = new HashMap<>();
	Map<String, Stat> undefined = new HashMap<>();

	@Test
	public void testSaveUser() {

		String query = "SELECT count(description) FROM top_images_final";
		Long total = jdbcTemplate.queryForObject(query, Long.class);
		System.out.printf("Total descriptions : %,d%n", total);
		WikiLangConverter.DEBUG = true;
		parseAndProcessDescriptions();
		System.out.printf("Total valid code : %d%n", valid.size());
		System.out.println("lang code,  count  , pageId , description");
		valid.entrySet().stream()
				.sorted(Comparator.comparingInt((Map.Entry<String, Stat> e) -> e.getValue().count).reversed())
				.forEach(e -> {
					Stat stat = e.getValue();
					System.out.println(e.getKey() + ", " + stat.count + ", " + stat.pageId + ", \"" + stat.description + "\"");
				});
		System.out.printf("Total undefined code : %d%n", undefined.size());
		System.out.println("lang code,  count  , pageId , description");
		undefined.entrySet().stream()
				.sorted(Comparator.comparingInt((Map.Entry<String, Stat> e) -> e.getValue().count).reversed())
				.forEach(e -> {
					Stat stat = e.getValue();
					System.out.println(e.getKey() + ", " + stat.count + ", " + stat.pageId + ", \"" + stat.description + "\"");
				});
	}

	public void parseAndProcessDescriptions() {
		String sql = "SELECT description, mediaId FROM top_images_final";

		jdbcTemplate.query(sql, rs -> {
			String description = rs.getString(1);
			String pageId = rs.getString(2);
			normalizeLang(description, pageId);
		});
	}

	public String normalizeLang(String jsonStr, String pageId) {
		if (jsonStr == null || jsonStr.trim().isEmpty()) {
			return "{}";
		}
		try {
			Map<String, String> parsed = gson.fromJson(jsonStr, mapType);
			if (parsed == null || parsed.isEmpty()) {
				return "{}";
			}
			if (++count % 100000 == 0) {
				System.out.println("Parsing: " + count + " valid code " + valid.size() + " undefined code " + undefined.size());
			}

			Map<String, String> normalized = new HashMap<>();
			for (Map.Entry<String, String> e : parsed.entrySet()) {
				String wikiLangCode = e.getKey().trim();
				String description = e.getValue();
				String bcp47 = WikiLangConverter.toBcp47FromWiki(wikiLangCode);
				if (bcp47.startsWith("und:")) {
					bcp47 = bcp47.replaceFirst("und:", "");
					undefined.merge(bcp47, new Stat(description, pageId), (oldVal, newVal) -> oldVal.incr());
				} else {
					valid.merge(bcp47, new Stat(description, pageId), (oldVal, newVal) -> oldVal.incr());
				}
				normalized.put(bcp47, description);
			}
			return gson.toJson(normalized);
		} catch (Exception ex) {
			ex.printStackTrace();
			return jsonStr;
		}
	}

	static final class Stat {
		public int count;
		public String description;
		public String pageId;


		public Stat(String description, String pageId) {
			this.description = description;
			this.pageId = pageId;
			incr();
		}

		public Stat incr() {
			count++;
			return this;
		}
	}
}