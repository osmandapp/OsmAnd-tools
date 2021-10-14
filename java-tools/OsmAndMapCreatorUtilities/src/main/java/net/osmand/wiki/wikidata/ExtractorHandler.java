package net.osmand.wiki.wikidata;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.helpers.DefaultHandler;

public class ExtractorHandler extends DefaultHandler {
	private long startTime;
	private Locator locator;
	private boolean start;
	private boolean article;
	private String articleId;
	private int startNumberBefore;
	private int endNumberBefore;
	private int startNumberAfter;
	private int endNumberAfter;
	private final String title;


	private final StringBuilder currentValue = new StringBuilder();

	public ExtractorHandler(String title) {
		this.title = title;
	}

	@Override
	public void setDocumentLocator(final Locator locator) {
		this.locator = locator;
	}

	@Override
	public void startDocument() {
		System.out.println("Start Document");
		startTime = System.currentTimeMillis();
	}

	@Override
	public void endDocument() {
		System.out.println("End Document. Parsing time: " + (System.currentTimeMillis() - startTime) / 1000);
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) {

		currentValue.setLength(0);

		if (qName.equals("page")) {
			if (!start) {
				start = true;
				startNumberBefore = locator.getLineNumber();
			}
			if (article) {
				article = false;
				startNumberAfter = locator.getLineNumber();
			}
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) {

		if (qName.equalsIgnoreCase("title")) {
			if (currentValue.toString().startsWith(title)) {
				endNumberBefore = locator.getLineNumber() - 1;
				System.out.println(locator.getLineNumber() + " " + currentValue);
				article = true;
			}
		}
		if (qName.equalsIgnoreCase("page")) {
			endNumberAfter = locator.getLineNumber();
		}
		if (qName.equalsIgnoreCase("id")) {
			if (article) {
				if (articleId == null) {
					articleId = currentValue.toString();
				}
			}
		}
	}

	@Override
	public void characters(char[] ch, int start, int length) {
		currentValue.append(ch, start, length);
	}

	public int getStartNumberBefore() {
		return startNumberBefore;
	}

	public int getEndNumberBefore() {
		return endNumberBefore;
	}

	public int getStartNumberAfter() {
		return startNumberAfter;
	}

	public int getEndNumberAfter() {
		return endNumberAfter;
	}

	public String getArticleId() {
		return articleId;
	}
}

