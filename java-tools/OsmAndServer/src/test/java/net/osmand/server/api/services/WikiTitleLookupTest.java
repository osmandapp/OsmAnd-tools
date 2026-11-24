package net.osmand.server.api.services;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

// wiki.wiki_mapping keeps Wikidata sitelink titles, which have spaces where article URLs have underscores
public class WikiTitleLookupTest {

	private static final String KULTURFORUM_ID = "386482";
	private static final String CPP_ID = "2407";
	private static final String BATMAN_SERIES_ID = "837286";

	private WikiService wikiService;

	@Before
	public void setUp() {
		JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
		when(jdbcTemplate.queryForObject(anyString(), eq(String.class), eq("de"), eq("Kulturforum Berlin")))
				.thenReturn(KULTURFORUM_ID);
		when(jdbcTemplate.queryForObject(anyString(), eq(String.class), eq("Kulturforum Berlin")))
				.thenReturn(KULTURFORUM_ID);
		when(jdbcTemplate.queryForObject(anyString(), eq(String.class), eq("en"), eq("C++")))
				.thenReturn(CPP_ID);
		when(jdbcTemplate.queryForObject(anyString(), eq(String.class), eq("en"), eq("Batman: The Animated Series")))
				.thenReturn(BATMAN_SERIES_ID);
		wikiService = new WikiService();
		wikiService.jdbcTemplate = jdbcTemplate;
	}

	@Test
	public void langTitleWithSpace() {
		assertEquals(KULTURFORUM_ID, wikiService.retrieveArticleIdFromWikiUrl("de:Kulturforum Berlin"));
	}

	@Test
	public void langTitleWithUnderscore() {
		assertEquals(KULTURFORUM_ID, wikiService.retrieveArticleIdFromWikiUrl("de:Kulturforum_Berlin"));
	}

	@Test
	public void langTitleWithColon() {
		assertEquals(BATMAN_SERIES_ID, wikiService.retrieveArticleIdFromWikiUrl("en:Batman:_The_Animated_Series"));
	}

	@Test
	public void titleWithoutLang() {
		assertEquals(KULTURFORUM_ID, wikiService.retrieveArticleIdFromWikiUrl("Kulturforum_Berlin"));
	}

	@Test
	public void articleUrl() {
		assertEquals(KULTURFORUM_ID, wikiService.retrieveArticleIdFromWikiUrl("https://de.wikipedia.org/wiki/Kulturforum_Berlin"));
	}

	@Test
	public void percentEncodedArticleUrl() {
		assertEquals(KULTURFORUM_ID, wikiService.retrieveArticleIdFromWikiUrl("https://de.wikipedia.org/wiki/Kulturforum%20Berlin"));
	}

	@Test
	public void plusInArticleUrlStaysPlus() {
		assertEquals(CPP_ID, wikiService.retrieveArticleIdFromWikiUrl("https://en.wikipedia.org/wiki/C++"));
		assertEquals(CPP_ID, wikiService.retrieveArticleIdFromWikiUrl("https://en.wikipedia.org/wiki/C%2B%2B"));
	}
}
