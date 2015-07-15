package net.osmand.data.preparation;

import gnu.trove.list.array.TLongArrayList;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import net.osmand.data.LatLon;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tools.bzip2.CBZip2InputStream;
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
	private static final boolean CREATE = false;
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
	
	public static class QueryData {
		ByteBuffer buf8 = ByteBuffer.allocate(8);
		ByteBuffer buf16 = ByteBuffer.allocate(16);
		TLongArrayList missing = new TLongArrayList();
		TLongArrayList ids = new TLongArrayList();
		
		double top = 0;
		double left = 0;
		double right = 0;
		double bottom = 0;
	}
	
	abstract static class DatabaseAdapter {
		File target;

		public DatabaseAdapter(File target) {
			this.target = target;
		}
		public abstract void prepareToCreate() throws Exception;
		
		public abstract void prepareToRead() throws Exception;
		
		public abstract void putBbox(long key, byte[] bbox) throws Exception;
		
		public abstract void commitBatch() throws Exception;
		
		public abstract void close() throws Exception;
		
		public abstract byte[] query(long id) throws Exception;
		
		public byte[] getBbox(QueryData qd) throws Exception {
			int top = 0;
			int left = 0;
			int right = 0;
			int bottom = 0;
			boolean first = true;
			qd.missing.clear();
			for(int i = 0; i < qd.ids.size(); i++){
				long id = qd.ids.get(i);
				byte[] bbox = query(id);
				if(bbox == null) {
					qd.missing.add(id);
					continue;
				}
				if(bbox.length == 8) {
					qd.buf8.clear();
					qd.buf8.put(bbox);
					qd.buf8.position(0);
					if(first) {
						first = false;
						top = bottom = qd.buf8.getInt();
						left = right = qd.buf8.getInt();
					} else {
						int y = qd.buf8.getInt();
						int x = qd.buf8.getInt();
						top = Math.max(y, top);
						bottom = Math.min(y, bottom);
						left = Math.min(x, left);
						right = Math.max(x, right);
					}
				} else if(bbox.length == 16) {
					qd.buf16.clear();
					qd.buf16.put(bbox);
					qd.buf16.position(0);
					int t = qd.buf16.getInt();
					int l = qd.buf16.getInt();
					int r = qd.buf16.getInt();
					int b = qd.buf16.getInt();
					if(first) {
						first = false;
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
			qd.top = top / 10000000.0;
			qd.bottom = bottom / 10000000.0;
			qd.left = left / 10000000.0;
			qd.right = right / 10000000.0;
			qd.buf16.clear();
			qd.buf16.putInt(top);
			qd.buf16.putInt(left);
			qd.buf16.putInt(right);
			qd.buf16.putInt(bottom);
			return qd.buf16.array();
		}
		
		public void put(long id, LatLon ll, QueryData qd) throws Exception {
			qd.buf8.clear();
			qd.buf8.putInt((int) (MapUtils.checkLatitude(ll.getLatitude()) * 10000000));
			qd.buf8.putInt((int) (MapUtils.checkLongitude(ll.getLongitude()) * 10000000));
			putBbox(id, qd.buf8.array());			
		} 
	}
	
	static class NullDatabaseTest extends DatabaseAdapter {
		public NullDatabaseTest(File target) {
			super(target);
		}
		
		@Override
		public void prepareToCreate() throws DBException, IOException, SQLException {
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

		@Override
		public void prepareToRead() throws Exception {
			
		}
	}
	
	static class SqliteDatabaseAdapter extends DatabaseAdapter {
		private int count;
		private DBDialect sqlite = DBDialect.SQLITE;
		private Connection db;
		private PreparedStatement ps;
		private PreparedStatement queryById;

		public SqliteDatabaseAdapter(File target) {
			super(target);
		}
		

		@Override
		public void prepareToRead() throws Exception {
			db = (Connection) sqlite.getDatabaseConnection(target.getAbsolutePath(), log);
			queryById = db.prepareStatement("select bbox from node where id = ?");
		}
		
		@Override
		public void prepareToCreate() throws DBException, IOException, SQLException {
			if(CREATE) {
				target.delete();
			}
			db = (Connection) sqlite.getDatabaseConnection(target.getAbsolutePath(), log);
			if(CREATE) {
				Statement stat = db.createStatement();
				stat.executeUpdate("create table node (id bigint primary key, bbox bytes)"); //$NON-NLS-1$
				stat.executeUpdate("create index IdIndex ON node (id)"); //$NON-NLS-1$
				stat.close();
			}
			ps = db.prepareStatement("insert or replace into node(id, bbox) values (?, ?)");
			queryById = db.prepareStatement("select bbox from node where id = ?");
			db.setAutoCommit(false);
		}

		@Override
		public void putBbox(long key, byte[] value) throws DBException, SQLException {
			ps.setLong(1, key);
			byte[] bs = new byte[value.length];
			System.arraycopy(value, 0, bs, 0, bs.length);
			ps.setBytes(2, bs);
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
			if (ps != null) {
				commitBatch();
				ps.close();
			}
			queryById.close();
			if(ps != null) {
				db.setAutoCommit(true);
				db.createStatement().execute("VACUUM");
			}
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
	
	static class LevelDbDatabaseAdapter extends DatabaseAdapter {

		private NativeDB open;
		private NativeWriteBatch updates;
		private int count;
		private ByteBuffer buffer;

		public LevelDbDatabaseAdapter(File target) {
			super(target);
		}
		
		@Override
		public void prepareToCreate() throws DBException, IOException {
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

		@Override
		public void prepareToRead() throws Exception {
		}
	}
	
	private static long nodeId(long id) {
		return id << 2;
	}
	
	private static long wayId(long id) {
		return (id << 2) | 1;
	}
	
	private static long relationId(long id) {
		return (id << 2) | 2;
	}
	
	private static long convertId(EntityId eid) {
		if(eid.getType() == EntityType.NODE) {
			return nodeId(eid.getId());
		} else if(eid.getType() == EntityType.WAY) {
			return wayId(eid.getId());
		} else {
			return relationId(eid.getId());
		}
	}
	
	
	public void splitRegionsOsc(String oscFile, String indexFile) throws Exception {
		OsmBaseStorage reader = new OsmBaseStorage();
		final DatabaseAdapter adapter = new SqliteDatabaseAdapter(new File(indexFile));
		adapter.prepareToRead();
		final QueryData qd = new QueryData();
		reader.getFilters().add(new IOsmStorageFilter() {

			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				qd.ids.add(convertId(entityId));
				return false;
			}
		});
		InputStream stream = new BufferedInputStream(new FileInputStream(oscFile), 8192 * 4);
		InputStream streamFile = stream;
		long st = System.currentTimeMillis();
		if (oscFile.endsWith(".bz2")) { //$NON-NLS-1$
			if (stream.read() != 'B' || stream.read() != 'Z') {
//				throw new RuntimeException("The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
			} else {
				stream = new CBZip2InputStream(stream);
			}
		} else if (oscFile.endsWith(".gz")) { //$NON-NLS-1$
			stream = new GZIPInputStream(stream);
		}
		reader.parseOSM(stream, new ConsoleProgressImplementation(), streamFile, false);
		adapter.getBbox(qd);
		System.out.println("Bbox " + qd.top + ", " + qd.left + " - " + qd.bottom + ", " + qd.right);
		adapter.close();
	}


	public void createIdToBBoxIndex(String location, String filename) throws FileNotFoundException, Exception,
			IOException {
		FileInputStream fis = new FileInputStream(location);
		// 1 null bbox for ways, 2 relation null bbox, 3 relations not processed because of loop
		final int[] stats = new int[] {0, 0, 0};
//		final DatabaseTest test = new NullDatabaseTest(new File(folder, filename + ".leveldb"));
//		final DatabaseTest test = new LevelDbDatabaseTest(new File(folder, filename + ".leveldb"));
		final DatabaseAdapter processor = new SqliteDatabaseAdapter(new File(filename));
		OsmBaseStoragePbf pbfReader = new OsmBaseStoragePbf();
		processor.prepareToCreate();
		final QueryData qd = new QueryData();
		final List<Relation> pendingRelations = new ArrayList<Relation>();
		pbfReader.getFilters().add(new IOsmStorageFilter() {
			long time = 0;
			long progress = 0;
			int count = 0;
			private boolean firstWay = true;
			private boolean firstRelation = true;
			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				if(time == 0) {
					time = System.currentTimeMillis();
				}
				try {
					if(entity instanceof Node) {
						if(CREATE) {
							// TODO
							processor.put( nodeId(entity.getId()), entity.getLatLon(), qd );
						}
						progress++;
						count ++;
					} else if(entity instanceof Way) {
						if(firstWay) {
							time = System.currentTimeMillis();
							firstWay = false;
							processor.commitBatch();
							progress = 0;
						}
						Way w = (Way) entity;
						qd.ids.clear();
						for(int i = 0; i < w.getNodeIds().size(); i++) {
							qd.ids.add(nodeId(w.getNodeIds().get(i)));
						}
						byte[] bbox = processor.getBbox(qd);
						if(bbox != null) {
							processor.putBbox(wayId(w.getId()), bbox);
						} else {
							stats[0]++;
						}
						progress++;
						count ++;
					} else if(entity instanceof Relation) {
						if(firstRelation) {
							time = System.currentTimeMillis();
							firstRelation = false;
							processor.commitBatch();
							progress = 0;
						}
						processRelations(stats, processor, pendingRelations, entity, qd);
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
		while(!pendingRelations.isEmpty()) {
			processor.commitBatch();
			int sz = pendingRelations.size();
			ArrayList<Relation> npendingRelations = new ArrayList<Relation>(sz);
			for(Relation r : pendingRelations) {
				processRelations(stats, processor, npendingRelations, r, qd);
			}
			if(npendingRelations.size() == sz) {
				for(Relation r : pendingRelations) {
					processRelations(stats, processor, null, r, qd);
				}	
				break;
			}
			pendingRelations.clear();
			pendingRelations.addAll(npendingRelations);
		}
		System.out.println("Error stats: " + stats[0] + " way null bbox, " + 
				stats[1] + " relation null bbox, " + stats[2] + " relation in cycle");
		System.out.println("Comitting");
		processor.close();
		System.out.println("Done");
	}
	
	private void processRelations(final int[] stats, final DatabaseAdapter processor,
			final List<Relation> pendingRelations, Entity entity, QueryData qd) throws Exception {
		Relation r = (Relation) entity;
		Iterator<EntityId> iterator = r.getMemberIds().iterator();
		qd.ids.clear();
		while(iterator.hasNext()) {
			EntityId nid = iterator.next();
			qd.ids.add(convertId(nid));
		}
		byte[] bbox = processor.getBbox(qd);
		boolean relationMissing = false;
		for(long l : qd.missing.toArray()) {
			if(l % 4 == 2) {
				relationMissing = true;
				break;
			}
		}
		if(!relationMissing) {
			if(bbox != null) {
				processor.putBbox(relationId(r.getId()), bbox);
			} else {
				stats[1]++;
			}
		} else {
			if(pendingRelations == null) {
				if(bbox != null) {
					processor.putBbox(relationId(r.getId()), bbox);
				} else {
					stats[1]++;
				}
				stats[2]++;
			} else {
				pendingRelations.add(r)	;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		String operation = "";
//		operation = "query";
		operation = "osc";
		if(args.length > 0) {
			operation = args[0];
		}
//		String location = "/Users/victorshcherb/osmand/temp/Netherlands-noord-holland.pbf";
		String location = "/Users/victorshcherb/osmand/temp/Netherlands-noord-holland.osc";
//		String location = "/Users/victorshcherb/osmand/temp/010.osc.gz";
		
		if (args.length > 1) {
			location = args[1];
		}
		String dbPath = new File(location).getParentFile() + "/bboxbyid.sqlite";
		if (args.length > 2) {
			location = args[2];
		}
		// create
		if (operation.equals("create")) {
			new IndexIdByBbox().createIdToBBoxIndex(location, dbPath);
		} else if (operation.equals("osc")) {
			new IndexIdByBbox().splitRegionsOsc(location, dbPath);
		} else if (operation.equals("")) {
			final DatabaseAdapter processor = new SqliteDatabaseAdapter(new File(dbPath));
			processor.prepareToRead();
			QueryData qd = new QueryData();
//			qd.ids.add(relationId(271110));
//			qd.ids.add(wayId(93368155l));
//			qd.ids.add(nodeId(2042972578l));
			processor.getBbox(qd);
			System.out.println("Bbox " + qd.top + ", " + qd.left + " - " + qd.bottom + ", " + qd.right);
			processor.close();
		}
	}
}
