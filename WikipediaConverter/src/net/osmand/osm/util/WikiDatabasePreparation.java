package net.osmand.osm.util;

import info.bliki.wiki.filter.HTMLConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import net.osmand.PlatformUtil;
import net.osmand.data.preparation.DBDialect;
import net.osmand.impl.ConsoleProgressImplementation;

import org.apache.commons.logging.Log;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class WikiDatabasePreparation {
	private static final Log log = PlatformUtil.getLog(WikiDatabasePreparation.class);
	
	public interface InsertValueProcessor {
    	public void process(List<String> vs);
    }
	
	public static class LatLon {
		private final double longitude;
		private final double latitude;

		public LatLon(double latitude, double longitude) {
			this.latitude = latitude;
			this.longitude = longitude;
		}
		
		public double getLatitude() {
			return latitude;
		}
		
		public double getLongitude() {
			return longitude;
		}

	}
    
    
	public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, SQLException {
//		String lang = "be";
//		String folder = "/home/victor/projects/osmand/wiki/";
		String lang = args[0];
		String folder  = "";
		if(args.length > 1){
			folder = args[1];
		}
		final String fileName = folder + lang + "wiki-latest-externallinks.sql.gz";
		final String wikiPg = folder + lang + "wiki-latest-pages-articles.xml.bz2";
		final String sqliteFileName = folder + lang + "wiki.sqlite";
		final WikiDatabasePreparation prep = new WikiDatabasePreparation();
    	
		Map<Long, LatLon> links = prep.parseExternalLinks(fileName);
		processWikipedia(wikiPg, lang, links, sqliteFileName);
//		testContent(lang, folder);
    }

	protected static void testContent(String lang, String folder) throws SQLException, IOException {
		Connection conn = DBDialect.SQLITE.getDatabaseConnection(folder + lang + "wiki.sqlite", log);
		ResultSet rs = conn.createStatement().executeQuery("SELECT * from wiki");
		while(rs.next()) {
			double lat = rs.getDouble("lat");
			double lon = rs.getDouble("lon");
			byte[] zp = rs.getBytes("zipContent");
			String title = rs.getString("title");
			BufferedReader rd = new BufferedReader(new InputStreamReader(
					new GZIPInputStream(new ByteArrayInputStream(zp))));
			System.out.println(title + " " + lat + " " + lon + " " + zp.length);
			String s ;
			while((s = rd.readLine()) != null) {
				System.out.println(s);
			}
		}
	}

	protected static void processWikipedia(final String wikiPg, String lang, Map<Long, LatLon> links, String sqliteFileName)
			throws ParserConfigurationException, SAXException, FileNotFoundException, IOException, SQLException {
		SAXParser sx = SAXParserFactory.newInstance().newSAXParser();
		InputStream streamFile = new BufferedInputStream(new FileInputStream(wikiPg), 8192 * 4);
		InputStream stream = streamFile;
		if (stream.read() != 'B' || stream.read() != 'Z') {
			throw new RuntimeException(
					"The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
		} 
		
		final WikiOsmHandler handler = new WikiOsmHandler(sx, streamFile, lang, links,  new File(sqliteFileName));
		sx.parse(new CBZip2InputStream(stream), handler);
		handler.finish();
	}

	protected Map<Long, LatLon> parseExternalLinks(final String fileName) throws IOException {
		final int[] total = new int[2];
    	final Map<Long, LatLon> pages = new LinkedHashMap<Long, LatLon>();
    	final Map<Long, Integer> counts = new LinkedHashMap<Long, Integer>();
    	InsertValueProcessor p = new InsertValueProcessor(){

			@Override
			public void process(List<String> vs) {
				final String link = vs.get(2);
				if (link.contains("geohack.php?")) {
					total[0]++;
					String paramsStr = link.substring(link.indexOf("params=") + "params=".length());
					paramsStr = strip(paramsStr, '&');
					String[] params = paramsStr.split("_");
					if(params[0].equals("")) {
						return;
					}
					int[] ind = new int[1];
					double lat = parseNumbers(params, ind, "n", "s");
					double lon = parseNumbers(params, ind, "e", "w");
					
					final long key = Long.parseLong(vs.get(1));
					int cnt = 1;
					if(counts.containsKey(key)) {
						cnt = counts.get(key) + 1;
					}
					counts.put(key, cnt);
 					if (pages.containsKey(key)) {
						final double dist = getDistance(pages.get(key).latitude, pages.get(key).longitude, lat, lon);
						if (dist > 10000) {
							System.err.println(key + " ? " + " dist = " + dist + " " + pages.get(key).latitude + "=="
									+ lat + " " + pages.get(key).longitude + "==" + lon);
							pages.remove(key);
						}
					} else {
						if(cnt == 1) {
							pages.put(key, new LatLon(lat, lon));
						}
					}
					total[1]++;
				}
			}

			protected String strip(String paramsStr, char c) {
				if(paramsStr.indexOf(c) != -1) {
					paramsStr = paramsStr.substring(0, paramsStr.indexOf(c));
				}
				return paramsStr;
			}


    		
    	};
		readInsertValuesFile(fileName, p);
		System.out.println("Found links for " + total[0] + ", parsed links " + total[1]);
		return pages;
	}
    
	protected double parseNumbers(String[] params, int[] ind, String pos, String neg) {
		String next = params[ind[0]++];
		double number = parseNumber(next);
		next = params[ind[0]++];
		if (next.length() > 0) {
			if (next.equalsIgnoreCase(pos) || next.equalsIgnoreCase(neg)) {
				if (next.equalsIgnoreCase(neg)) {
					number = -number;
				}
				return number;
			}
			double latmin = parseNumber(next);
			number += latmin / 60;
		}
		next = params[ind[0]++];
		if (next.length() > 0) {
			if (next.equalsIgnoreCase(pos) || next.equalsIgnoreCase(neg)) {
				if (next.equalsIgnoreCase(neg)) {
					number = -number;
				}
				return number;
			}
			double latsec = parseNumber(next);
			number += latsec / 3600;
		}
		next = params[ind[0]++];
		if (next.equalsIgnoreCase(pos) || next.equalsIgnoreCase(neg)) {
			if (next.equalsIgnoreCase(neg)) {
				number = -number;
				return number;
			}
		} else {
			throw new IllegalStateException();
		}

		return number;
	}

	protected double parseNumber(String params) {
		if(params.startsWith("--")) {
			params = params.substring(2);
		}
		return Double.parseDouble(params);
	}

	protected void readInsertValuesFile(final String fileName, InsertValueProcessor p)
			throws FileNotFoundException, UnsupportedEncodingException, IOException {
		InputStream fis = new FileInputStream(fileName);
		if(fileName.endsWith("gz")) {
			fis = new GZIPInputStream(fis);
		}
    	InputStreamReader read = new InputStreamReader(fis, "UTF-8");
    	char[] cbuf = new char[1000];
    	int cnt;
    	boolean values = false;
    	String buf = ""	;
    	List<String> insValues = new ArrayList<String>();
    	while((cnt = read.read(cbuf)) >= 0) {
    		String str = new String(cbuf, 0, cnt);
    		buf += str;
    		if(!values) {
    			if(buf.contains("VALUES")) {
    				buf = buf.substring(buf.indexOf("VALUES") + "VALUES".length()); 
    				values = true;
    			}
    		} else {
    			boolean openString = false;
    			int word = -1;
    			int last = 0;
    			for(int k = 0; k < buf.length(); k++) {
    				if(openString ) {
						if (buf.charAt(k) == '\'' && (buf.charAt(k - 1) != '\\' 
								|| buf.charAt(k - 2) == '\\')) {
    						openString = false;
    					}
    				} else if(buf.charAt(k) == ',' && word == -1) {
    					continue;
    				} else if(buf.charAt(k) == '(') {
    					word = k;
    					insValues.clear();
    				} else if(buf.charAt(k) == ')' || buf.charAt(k) == ',') {
    					String vl = buf.substring(word + 1, k).trim();
    					if(vl.startsWith("'")) {
    						vl = vl.substring(1, vl.length() - 1);
    					}
    					insValues.add(vl);
    					if(buf.charAt(k) == ')') {
//    						if(insValues.size() < 4) {
//    							System.err.println(insValues);
//    							System.err.println(buf);
//    						}
    						try {
								p.process(insValues);
							} catch (Exception e) {
								System.err.println(insValues);
//								e.printStackTrace();
							}
    						last = k + 1;
    						word = -1;
    					} else {
    						word = k;
    					}
    				} else if(buf.charAt(k) == '\'') {
    					openString = true;
    				}
    			}
    			buf = buf.substring(last);
    			
    			
    		}
    		
    	}
    	read.close();
	}
	
	public static class WikiOsmHandler extends DefaultHandler {
		long id = 1;
		private final SAXParser saxParser;
		private boolean page = false;
		private boolean revision = false;
		private StringBuilder ctext = null;
		private long cid;

		private StringBuilder title = new StringBuilder();
		private StringBuilder text = new StringBuilder();
		private StringBuilder pageId = new StringBuilder();
		private boolean parseText = false;
		private Map<Long, LatLon> pages = new HashMap<Long, LatLon>(); 

		private final InputStream progIS;
		private final HTMLConverter converter = new HTMLConverter(false);
		private WikiModel wikiModel ;
		private ConsoleProgressImplementation progress = new ConsoleProgressImplementation();
		private DBDialect dialect = DBDialect.SQLITE;
		private Connection conn;
		private PreparedStatement prep;
		private int batch = 0;
		private final static int BATCH_SIZE = 500;
		final ByteArrayOutputStream bous = new ByteArrayOutputStream(64000);
		

		WikiOsmHandler(SAXParser saxParser, InputStream progIS, String lang,
				Map<Long, LatLon> pages, File sqliteFile)
				throws IOException, SQLException{
			this.pages = pages;
			this.saxParser = saxParser;
			this.progIS = progIS;
			dialect.removeDatabase(sqliteFile);
			conn = (Connection) dialect.getDatabaseConnection(sqliteFile.getAbsolutePath(), log);
			conn.createStatement().execute("CREATE TABLE wiki(id long, lat double, lon double, title text, zipContent blob)");
			prep = conn.prepareStatement("INSERT INTO wiki VALUES (?, ?, ?, ?, ?)");
			
			
			this.wikiModel = new WikiModel("http://"+lang+".wikipedia.com/wiki/${image}", "http://"+lang+".wikipedia.com/wiki/${title}");
			progress.startTask("Parse wiki xml", progIS.available());
		}
		
		public void addBatch() throws SQLException {
			prep.addBatch();
			if(batch++ > BATCH_SIZE) {
				prep.executeBatch();
				batch = 0;
			}
		}
		
		public void finish() throws SQLException {
			prep.executeBatch();
			if(!conn.getAutoCommit()) {
				conn.commit();
			}
			prep.close();
			conn.close();
		}

		public int getCount() {
			return (int) (id - 1);
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			if (!page) {
				page = name.equals("page");
			} else {
				if (name.equals("title")) {
					title.setLength(0);
					ctext = title;
				} else if (name.equals("text")) {
					if(parseText) {
						text.setLength(0);
						ctext = text;
					}
				} else if (name.equals("revision")) {
					revision  = true;
				} else if (name.equals("id") && !revision) {
					pageId.setLength(0);
					ctext = pageId;
				}
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			if (page) {
				if (ctext != null) {
					ctext.append(ch, start, length);
				}
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			String name = saxParser.isNamespaceAware() ? localName : qName;
			try {
				if (page) {
					if (name.equals("page")) {
						page = false;
						parseText = false;
						progress.remaining(progIS.available());
					} else if (name.equals("title")) {
						ctext = null;
					} else if (name.equals("revision")) {
						revision = false;
					} else if (name.equals("id") && !revision) {
						ctext = null;
						cid = Long.parseLong(pageId.toString());
						parseText = pages.containsKey(cid);
					} else if (name.equals("text")) {
						if (parseText) {
							LatLon ll = pages.get(cid);
							if(id++ % 500 == 0) {
								log.debug("Article accepted " + cid + " " + title.toString() + " " + ll.getLatitude() + " " + ll.getLongitude());
							}
							String plainStr = wikiModel.render(converter, ctext.toString());
							try {
								prep.setLong(1, cid);
								prep.setDouble(2, ll.getLatitude());
								prep.setDouble(3, ll.getLongitude());
								prep.setString(4, title.toString());
								bous.reset();
								GZIPOutputStream gzout = new GZIPOutputStream(bous);
								gzout.write(plainStr.getBytes("UTF-8"));
								gzout.close();
								final byte[] byteArray = bous.toByteArray();
								prep.setBytes(5, byteArray);
								addBatch();
							} catch (SQLException e) {
								throw new SAXException(e);
							}
						}
						ctext = null;
					}
				}
			} catch (IOException e) {
				throw new SAXException(e);
			}
		}
		
		
	}
	
	
	/**
	 * Gets distance in meters
	 */
	public static double getDistance(double lat1, double lon1, double lat2, double lon2){
		double R = 6372.8; // for haversine use R = 6372.8 km instead of 6371 km
		double dLat = toRadians(lat2-lat1);
		double dLon = toRadians(lon2-lon1); 
		double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
		        Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * 
		        Math.sin(dLon/2) * Math.sin(dLon/2); 
		//double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
		//return R * c * 1000;
		// simplyfy haversine:
		return (2 * R * 1000 * Math.asin(Math.sqrt(a)));
	}
	
	private static double toRadians(double angdeg) {
//		return Math.toRadians(angdeg);
		return angdeg / 180.0 * Math.PI;
	}
		
}