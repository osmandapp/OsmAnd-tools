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
	private int endHeaderLine;
	private int firstArticleLine;
	private int endArticleLine;
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
				endHeaderLine = locator.getLineNumber();
			}
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName) {

		if (qName.equalsIgnoreCase("title")) {
			if (currentValue.toString().startsWith(title)) {
				firstArticleLine = locator.getLineNumber() - 1;
				System.out.println(locator.getLineNumber() + " " + currentValue);
				article = true;
			}
		}
		if (qName.equals("page")) {
			if (article) {
				article = false;
				endArticleLine = locator.getLineNumber();
				System.out.println("Parsing time: " + (System.currentTimeMillis() - startTime) / 1000);
				throw new StopParsingException();
			}
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

	public int getEndHeaderLine() {
		return endHeaderLine;
	}

	public int getFirstArticleLine() {
		return firstArticleLine;
	}

	public int getEndArticleLine() {
		return endArticleLine;
	}

	public String getArticleId() {
		return articleId;
	}

	static class StopParsingException extends RuntimeException {
	}
}

