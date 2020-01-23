package net.osmand;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;

import net.osmand.PlatformUtil;
import net.osmand.map.ITileSource;
import net.osmand.map.TileSourceManager.TileSourceTemplate;

import org.apache.commons.logging.Log;


public class SQLiteBigPlanetIndex {
	private static final Log log = PlatformUtil.getLog(SQLiteBigPlanetIndex.class);

	private static final int BATCH_SIZE = 50;
	private static final int LOG_INFO = 10000;
	private static boolean bigPlanet = false;
	
	public static void main(String[] subArgsArray) throws SQLException, IOException {
		SQLiteParams params = new SQLiteParams();
		boolean zoomSpecified = false;
		for(int i = 0; i < subArgsArray.length; i++) {
			String param = subArgsArray[i];
			if (param.startsWith("--")) {
				String[] s = subArgsArray[i].split("=");
				String key = s[0];
				String value = "";
				if (s.length > 1) {
					value = s[1];
				}
				switch (key) {
				case "--help":
					help(null);
					return;
				case "--minzoom":
					zoomSpecified = true;
					params.minZoom = Integer.parseInt(value);
					break;
				case "--maxzoom":
					zoomSpecified = true;
					params.maxZoom = Integer.parseInt(value);
					break;
				case "--urltemplate":
					params.urlTemplate = value;
					break;
				case "--rule":
					params.rule = value;
					break;
				case "--referrer":
					params.referrer = value;
					break;
				case "--randoms":
					params.randoms = value;
					break;
				case "--inverted_y":
					params.invertedY = true;
					break;
				case "--bigplanet":
					params.bigPlanet = true;
					break;
				case "--ellipsoid":
					params.ellipsoid = true;
					break;
				case "--expireminutes":
					params.expireminutes = Integer.parseInt(value);
					break;
				default:
					break;
				}
			} else if(params.tilesFolder == null) {
				params.tilesFolder = new File(param);
			} else {
				params.fileToWrite = new File(param);
			}
		}
		if(params.tilesFolder == null || !params.tilesFolder.exists()) {
			help("Tiles folder is not specified");
			return;
		}
		if(!zoomSpecified) {
			TreeSet<Integer> tint = new TreeSet<>();
			for(File f : params.tilesFolder.listFiles()) {
				if(f.isDirectory()) {
					try {
						tint.add(Integer.parseInt(f.getName()));
					} catch (RuntimeException e) {
					}
				}
			}
			if(tint.isEmpty()) {
				help("Tiles folder doesn't have any zooms");
				return;
			}
			params.minZoom = tint.first();
			params.maxZoom = tint.last();
		}
		
		if(params.fileToWrite == null) {
			params.fileToWrite = new File(params.tilesFolder.getParentFile(), params.tilesFolder.getName() + ".sqlitedb");
		}
		createSQLiteDatabase(params);
	}
	
	private static void help(String err) {
		if(err != null) {
			System.out.println("ERROR: " + err);
		}
		System.out.println("This utility creates .sqlitedb from directory with tiles that could be used as Overlay/Underlay in OsmAnd: ");
		System.out.println("\t SYNOPSYS: create-sqlitedb <dir-with-tiles> [options] <optional sqlitedbfile>");
		System.out.println("\t OPTIONS:");
		System.out.println("\t\t --minzoom=: minzoom for sqlitedb, otherwise determined automatically");
		System.out.println("\t\t --maxzoom=: maxzoom for sqlitedb, otherwise determined automatically");
		
		System.out.println("\t\t --inverted_y: inverts y, while insert and request inverted y for url");
		System.out.println("\t\t --expireminutes: minutes expiration to redownload tile");
		System.out.println("\t\t --ellipsoid: ellipsoid mercator projection");
		System.out.println("\t\t --urltemplate=: url template to download new tiles");
		System.out.println("\t\t --rule=: rule how url templated is parsed (values <empty>, <template:1>)");
		System.out.println("\t\t --referrer=: referrer to be used while downloading new tiles");
		System.out.println("\t\t --randoms: randoms array used by urltemplate");
		
	}

	public static class SQLiteParams {
		public boolean invertedY;
		public boolean ellipsoid;
		public int expireminutes;
		public boolean bigPlanet = false;
		public int maxZoom = 18;
		public int minZoom = 4;
		public String urlTemplate = null;
		public String referrer;
		public String randoms;
		public String rule;
		public File fileToWrite;
		public File tilesFolder;
		
		
		public SQLiteParams(File dirWithTiles, String regionName, ITileSource template) {
			fileToWrite = new File(dirWithTiles, regionName + "." + template.getName() + ".sqlitedb");
			tilesFolder = new File(dirWithTiles, template.getName());
			minZoom = template.getMinimumZoomSupported();
			maxZoom = template.getMaximumZoomSupported();
			urlTemplate = ((TileSourceTemplate) template).getUrlTemplate();
			rule = ((TileSourceTemplate) template).getRule();
			referrer = ((TileSourceTemplate) template).getReferer();
			randoms = ((TileSourceTemplate) template).getRandoms();
			expireminutes = ((TileSourceTemplate) template).getExpirationTimeMinutes();
			invertedY = ((TileSourceTemplate) template).isInvertedYTile();
			ellipsoid = ((TileSourceTemplate) template).isEllipticYTile();
		}
		
		public SQLiteParams() {
		}
		
		public File getTilesFolder() {
			return tilesFolder;
		}
		
		public File getFileToWrite() {
			return fileToWrite;
		}
		
		public int getMaxZoom() {
			return maxZoom;
		}
		
		public int getMinZoom() {
			return minZoom;
		}
		
	}
	

	public static void createSQLiteDatabase(SQLiteParams params) throws SQLException, IOException {
		long now = System.currentTimeMillis();
		int COUNTER = 0; 
		try {
			Class.forName("org.sqlite.JDBC"); //$NON-NLS-1$
		} catch (ClassNotFoundException e) {
			log.error("Illegal configuration", e); //$NON-NLS-1$
			throw new IllegalStateException(e);
		}
		params.fileToWrite.delete();
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + params.fileToWrite.getAbsolutePath()); //$NON-NLS-1$
		Statement statement = conn.createStatement();
		statement.execute("CREATE TABLE tiles (x int, y int, z int, s int, image blob, time long, PRIMARY KEY (x,y,z,s))");
		statement.execute("CREATE INDEX IND on tiles (x,y,z,s)");
		statement.execute("CREATE TABLE info(tilenumbering,minzoom,maxzoom,timecolumn,url,rule,referer,ellipsoid,inverted_y,expireminutes,randoms)");
		statement.execute("CREATE TABLE android_metadata (locale TEXT)");
		statement.close();


		PreparedStatement pStatement = conn.prepareStatement("INSERT INTO INFO VALUES(?,?,?,?,?,?,?,?,?,?,?)");
		String tileNumbering = params.bigPlanet ? "BigPlanet" : "simple";
		pStatement.setString(1, tileNumbering);
		int minNormalZoom = bigPlanet ? 17 - params.getMaxZoom() : params.getMinZoom();
		int maxNormalZoom = bigPlanet ? 17 - params.getMinZoom() : params.getMaxZoom();
		pStatement.setInt(2, minNormalZoom);
		pStatement.setInt(3, maxNormalZoom);
		pStatement.setString(4, "yes");
		pStatement.setString(5, params.urlTemplate);
		pStatement.setString(6, params.rule);
		pStatement.setString(7, params.referrer);
		pStatement.setInt(8, params.ellipsoid? 1 : 0);
		pStatement.setInt(9, params.invertedY? 1 : 0);
		pStatement.setInt(10, params.expireminutes);
		pStatement.setString(11, params.randoms);
		pStatement.execute();
		log.info("Info table " + tileNumbering + "maxzoom = " + maxNormalZoom + " minzoom = " + minNormalZoom + " timecolumn = yes"
				+ " url = " + params.urlTemplate);
		pStatement.close();


		conn.setAutoCommit(false);
		pStatement = conn.prepareStatement("INSERT INTO tiles VALUES (?, ?, ?, ?, ?, ?)");
		int ch = 0;
		// be attentive to create buf enough for image
		byte[] buf;
		int maxZoom = 17;
		int minZoom = 1;

		File rootDir = params.getTilesFolder();
		for(File z : rootDir.listFiles()){
			try {
				int zoom = Integer.parseInt(z.getName());
				for(File xDir : z.listFiles()){
					try {
						int x = Integer.parseInt(xDir.getName());
						for(File f : xDir.listFiles()){
							if(!f.isFile()){
								continue;
							}
							try {
								int i = f.getName().indexOf('.');
								int y = Integer.parseInt(f.getName().substring(0, i));
								if(params.invertedY) {
									y = (1 << zoom) - 1 - y;
								}
								
								buf = new byte[(int) f.length()];
								if(zoom > maxZoom){
									maxZoom = zoom;
								}
								if(zoom < minZoom){
									minZoom = zoom;
								}

								FileInputStream is = new FileInputStream(f);
								int l = 0;
								try {
									l = is.read(buf);
								} finally {
									is.close();
								}
								if (l > 0) {
									pStatement.setInt(1, x);
									pStatement.setInt(2, y);
									pStatement.setInt(3, bigPlanet ? 17 - zoom : zoom);
									pStatement.setInt(4, 0);
									pStatement.setBytes(5, buf);
									pStatement.setLong(6, f.lastModified());
									pStatement.addBatch();
									ch++;
									if (++COUNTER % LOG_INFO == 0) {
										log.info(String.format("%d tiles are inserted", COUNTER));
									}
									if (ch >= BATCH_SIZE) {
										pStatement.executeBatch();
										ch = 0;
									}
								}

							} catch (NumberFormatException e) {
							}
						}
					} catch (NumberFormatException e) {
					}
				}

			} catch (NumberFormatException e) {
			}

		}

		if (ch > 0) {
			pStatement.executeBatch();
			ch = 0;
		}

		pStatement.close();
		conn.commit();
		conn.close();
		float seconds = (System.currentTimeMillis() - now) / 1000.0f;
		log.info("Index created " + params.getFileToWrite().getName() + " " + seconds + " s");
	}


	

}
