package net.osmand.data.preparation;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fusesource.leveldbjni.internal.NativeComparator;
import org.fusesource.leveldbjni.internal.NativeCompressionType;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.fusesource.leveldbjni.internal.NativeDB.DBException;
import org.fusesource.leveldbjni.internal.NativeOptions;
import org.fusesource.leveldbjni.internal.NativeWriteBatch;
import org.fusesource.leveldbjni.internal.NativeWriteOptions;

public class IndexIdByBbox {
	private static final Log log = LogFactory.getLog(IndexIdByBbox.class);
	private static final int BATCH_SIZE = 100000;
	public static byte[] longToBytes(long l) {
	    byte[] result = new byte[8];
	    for (int i = 7; i >= 0; i--) {
	        result[i] = (byte)(l & 0xFF);
	        l >>= 8;
	    }
	    return result;
	}

	public static long bytesToLong(byte[] b) {
	    long result = 0;
	    for (int i = 0; i < 8; i++) {
	        result <<= 8;
	        result |= (b[i] & 0xFF);
	    }
	    return result;
	}
	
	abstract static class DatabaseTest {
		File target;

		public DatabaseTest(File target) {
			this.target = target;
		}
		public abstract void prepare() throws Exception;
		
		public abstract void putBbox(long key, byte[] bbox) throws Exception;
		
		public abstract void close() throws Exception;
		
		public abstract void commitBatch() throws Exception;
		
		
		public byte[] getBbox(TLongArrayList ids, ByteBuffer buf8, ByteBuffer buf16) throws Exception {
			int top = 0;
			int left = 0;
			int right = 0;
			int bottom = 0;
			boolean first = true;
			for(int i = 0; i < ids.size(); i++){
				byte[] bbox = query(ids.get(i));
				if(bbox == null) {
					continue;
				}
				if(bbox.length == 8) {
					buf8.clear();
					buf8.put(bbox);
					buf8.position(0);
					if(first) {
						first = false;
						top = bottom = buf8.getInt();
						left = right = buf8.getInt();
					} else {
						int y = buf8.getInt();
						int x = buf8.getInt();
						top = Math.max(y, top);
						bottom = Math.min(y, bottom);
						left = Math.min(x, left);
						right = Math.max(y, right);
					}
				} else if(bbox.length == 16) {
					buf16.clear();
					buf16.put(bbox);
					buf16.position(0);
					int t = buf8.getInt();
					int l = buf8.getInt();
					int r = buf8.getInt();
					int b = buf8.getInt();
					if(first) {
						bottom = b;
						top = t;
						left = l;
						right = r;
					} else {
						top = Math.max(t, top);
						bottom = Math.min(b, bottom);
						left = Math.min(l, left);
						right = Math.max(r, right);
					}
				} else {
					throw new UnsupportedOperationException();
				}
				
			}
			if(first) {
				return null;
			}
			buf16.clear();
			buf16.putInt(top);
			buf16.putInt(left);
			buf16.putInt(right);
			buf16.putInt(bottom);
			return buf16.array();
		}
		public abstract byte[] query(long id) throws Exception; 
	}
	
	static class NullDatabaseTest extends DatabaseTest {
		public NullDatabaseTest(File target) {
			super(target);
		}
		
		@Override
		public void prepare() throws DBException, IOException, SQLException {
		}

		@Override
		public void putBbox(long key, byte[] value) throws DBException, SQLException {
		}

		@Override
		public void close() throws Exception {
		}

		@Override
		public void commitBatch() throws Exception {
		}

		@Override
		public byte[] query(long id) {
			return null;
		}
	}
	
	static class SqliteDatabaseTest extends DatabaseTest {
		private int count;
		private DBDialect sqlite = DBDialect.SQLITE;
		private Connection db;
		private PreparedStatement ps;
		private PreparedStatement queryById;

		public SqliteDatabaseTest(File target) {
			super(target);
		}
		
		@Override
		public void prepare() throws DBException, IOException, SQLException {
			target.delete();
			db = (Connection) sqlite.getDatabaseConnection(target.getAbsolutePath(), log);
			Statement stat = db.createStatement();
			stat.executeUpdate("create table node (id bigint primary key, bbox bytes)"); //$NON-NLS-1$
			stat.executeUpdate("create index IdIndex ON node (id)"); //$NON-NLS-1$
			stat.close();
			ps = db.prepareStatement("insert into node(id, bbox) values (?, ?)");
			queryById = db.prepareStatement("select bbox from node where id = ?");
			db.setAutoCommit(false);
		}

		@Override
		public void putBbox(long key, byte[] value) throws DBException, SQLException {
			ps.setLong(1, key);
			ps.setBytes(2, value);
			ps.addBatch();
			count++;
			if (count >= BATCH_SIZE) {
				commitBatch();
			}
		}

		@Override
		public void commitBatch() throws SQLException {
			ps.executeBatch();
			db.commit();
			count = 0;
		}

		@Override
		public void close() throws Exception {
			commitBatch();
			ps.close();
			queryById.close();
			db.setAutoCommit(true);
			db.createStatement().execute("VACUUM");
			db.close();
		}

		@Override
		public byte[] query(long id) throws SQLException {
			queryById.setLong(1, id);
			ResultSet rs = queryById.executeQuery();
			if(rs.next()) {
				return rs.getBytes(1);
			}
			return null;
		}
	}
	
	static class LevelDbDatabaseTest extends DatabaseTest {

		private NativeDB open;
		private NativeWriteBatch updates;
		private int count;
		private ByteBuffer buffer;

		public LevelDbDatabaseTest(File target) {
			super(target);
		}
		
		@Override
		public void prepare() throws DBException, IOException {
			NativeOptions no = new NativeOptions().
					createIfMissing(true).
					compression(NativeCompressionType.kSnappyCompression).
//					cache(new NativeCache(1024)).
					comparator(new NativeComparator() {

						@Override
						public int compare(byte[] arg0, byte[] arg1) {
							for(int i = 0; i < arg0.length && i < arg1.length; i++) {
								if(arg0[i] < arg1[i]) {
									return -1;
								} else if(arg1[i] < arg0[i]) {
									return 1;
								}
							}
							return 0;
						}

						@Override
						public String name() {
							return "Natural long";
						}
						
					});
			open = NativeDB.open(no, target);
			updates = new NativeWriteBatch();
			buffer = ByteBuffer.allocate(8);
		}

		@Override
		public void putBbox(long key, byte[] value) throws Exception {
			buffer.clear();
			buffer.putLong(key);
			updates.put(buffer.array(), value);
			count++;
			if (count >= BATCH_SIZE) {
				commitBatch();	
			}
		}

		@Override
		public void close() throws Exception {
			commitBatch();
		}

		@Override
		public void commitBatch() throws Exception {
			if (count > 0) {
				final NativeWriteOptions opts = new NativeWriteOptions();
				opts.sync(false);
				open.write(opts, updates);
				updates.clear();
				count = 0;
			}
		}

		@Override
		public byte[] query(long id) {
			return null;
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		String location = "/Users/victorshcherb/osmand/temp/Netherlands-noord-holland.pbf";
		if(args.length > 0) {
			location = args[0];
		}
		File folder = new File(location).getParentFile();
		FileInputStream fis = new FileInputStream(location);
//		final DatabaseTest test = new NullDatabaseTest(new File(folder, "bboxbyid.leveldb"));
//		final DatabaseTest test = new LevelDbDatabaseTest(new File(folder, "bboxbyid.leveldb"));
		final DatabaseTest processor = new SqliteDatabaseTest(new File(folder, "bboxbyid.sqlite"));
		OsmBaseStoragePbf pbfReader = new OsmBaseStoragePbf();
		processor.prepare();
		pbfReader.getFilters().add(new IOsmStorageFilter() {
			long time = 0;
			int count = 0;
			int progress = 0;
			ByteBuffer buf8 = ByteBuffer.allocate(8);
			ByteBuffer buf16 = ByteBuffer.allocate(16);
			TLongArrayList ids = new TLongArrayList();
			private boolean firstWay = true;
			
			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				if(time == 0) {
					time = System.currentTimeMillis();
				}
				try {
					if(entity instanceof Node) {
						LatLon ll = entity.getLatLon();
						buf8.clear();
						buf8.putInt((int) (MapUtils.checkLatitude(ll.getLatitude()) * 10000000));
						buf8.putInt((int) (MapUtils.checkLatitude(ll.getLongitude()) * 10000000));
						processor.putBbox(entity.getId() << 2, buf8.array());
						progress++;
						count ++;
					} else if(entity instanceof Way) {
						if(firstWay) {
							time = System.currentTimeMillis();
							firstWay = false;
							processor.commitBatch();
						}
						Way w = (Way) entity;
						ids.clear();
						for(int i = 0; i < w.getNodeIds().size(); i++) {
							ids.add(w.getNodeIds().get(i) << 2);
						}
						byte[] bbox = processor.getBbox(ids, buf8, buf16);
						processor.putBbox((w.getId() << 2) | 1, bbox);
						progress++;
						count ++;
					}
					if (count >= BATCH_SIZE) {
						count = 0;
						System.out.println("Progress " + progress + " "
								+ (progress * 1000l / (System.currentTimeMillis() - time)) + " rec/second");
					}
					return false;
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		});
		
		((OsmBaseStoragePbf) pbfReader).parseOSMPbf(fis, new ConsoleProgressImplementation(), false);
		fis.close();
		System.out.println("Comitting");
		processor.close();
		System.out.println("Done");
		
	}
}
