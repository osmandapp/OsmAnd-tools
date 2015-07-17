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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.LatLon;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.util.Algorithms;
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
import org.xml.sax.SAXException;

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
	
	
	static class Boundary {
		static final int VL = -500; 
		int top = VL;
		int left = VL;
		int right = VL;
		int bottom = VL;
		Boundary next;
		static final int DIST_O = 5 * 10000000;
		
		public void clear() {
			next = null;
			top = left = right = bottom = VL;
		}
		
		public void update(int x, int y) {
			update(y, x, y, x);
		}
		
		public void update(int t, int l, int b, int r) {
			if (VL == left && VL == bottom && top == VL && right == VL) {
				this.left = l;
				this.bottom = b;
				this.top = t;
				this.right = r;
			} else {
				boolean tooMuchDistance = false;
				tooMuchDistance = 
						(Math.abs(top - t) > DIST_O && Math.abs(bottom - t) > DIST_O) ||
						(Math.abs(top - b) > DIST_O && Math.abs(bottom - b) > DIST_O) ||
						(Math.abs(right - r) > DIST_O && Math.abs(left - r) > DIST_O) ||
						(Math.abs(right - l) > DIST_O && Math.abs(left - l) > DIST_O);
				if (tooMuchDistance) {
					if (next == null) {
						next = new Boundary();
					}
					next.update(t, l, b, r);
				} else {
					top = Math.max(t, top);
					bottom = Math.min(b, bottom);
					left = Math.min(l, left);
					right = Math.max(r, right);
				}
			}
		}
		
		public int depth() {
			if(next == null) {
				return 1;
			}
			return next.depth() + 1;
		}

		public boolean isEmpty() {
			return VL == left && VL == bottom  && top == VL && right == VL;
		}

		public void update(LatLon ll) {
			update(convertLon(ll.getLongitude()), convertLat(ll.getLatitude()));
		}
		
		public int getTop(boolean all) {
			if(next == null || !all) {
				return top;
			}
			return Math.max(top, next.getTop(all));
		}
		
		
		public int getRight(boolean all) {
			if(next == null || !all) {
				return right;
			}
			return Math.max(right, next.getRight(all));
		}
		
		public int getLeft(boolean all) {
			if(next == null || !all) {
				return left;
			}
			return Math.min(left, next.getLeft(all));
		}
		
		public int getBottom(boolean all) {
			if(next == null || !all) {
				return bottom;
			}
			return Math.min(bottom, next.getBottom(all));
		}
		
		public double getTopLat() {
			return convertToLat(getTop(false));
		}
		
		public double getBottomLat() {
			return convertToLat(getBottom(false));
		}
		
		public double getRightLon() {
			return convertToLon(getRight(false));
		}
		
		public double getLeftLon() {
			return convertToLon(getLeft(false));
		}

		public double convertToLat(int y) {
			return y / 10000000.0;
		}
		
		public double convertToLon(int x) {
			return x / 10000000.0;
		}
		
		public int convertLat(double latitude) {
			return (int) (MapUtils.checkLatitude(latitude) * 10000000);
		}
		
		public int convertLon(double longitude) {
			return (int) (MapUtils.checkLongitude(longitude) * 10000000);
		}

		public byte[] getBytes(ByteBuffer buf16) {
			if (next == null) {
				buf16.clear();
				buf16.putInt(top);
				buf16.putInt(left);
				buf16.putInt(right);
				buf16.putInt(bottom);
				return buf16.array();
			}
			byte[] l = new byte[depth() * 16];
			int sh = 0;
			Boundary b = this;
			while (b != null) {
				buf16.clear();
				buf16.putInt(top);
				buf16.putInt(left);
				buf16.putInt(right);
				buf16.putInt(bottom);
				System.arraycopy(buf16.array(), 0, l, sh, 16);
				b = b.next;
				sh += 16;
			}
			return l;
		}

		public String getBoundaryString() {
			String s = getTopLat() + ", " + getLeftLon() + " - "
					+ getBottomLat() + ", " + getRightLon();
			if(next != null) {
				s += "; " + next.getBoundaryString();
			}
			return  s;
		}
		
	}
	
	public static class QueryData {
		ByteBuffer buf8 = ByteBuffer.allocate(8);
		ByteBuffer buf16 = ByteBuffer.allocate(16);
		TLongArrayList missing = new TLongArrayList();
		TLongArrayList ids = new TLongArrayList();
		Boundary boundary = new Boundary();
		long queried;
		long written;
		
		
		public void updateBoundary(long id, byte[] bbox) {
			queried ++;
			if(bbox == null) {
				missing.add(id);
				return;
			}
			if (bbox.length == 8) {
				buf8.clear();
				buf8.put(bbox);
				buf8.position(0);
				int y = buf8.getInt();
				int x = buf8.getInt();
				boundary.update(x, y);
			} else if(bbox.length == 16) {
				buf16.clear();
				buf16.put(bbox);
				buf16.position(0);
				int t = buf16.getInt();
				int l = buf16.getInt();
				int r = buf16.getInt();
				int b = buf16.getInt();
				boundary.update(t, l, b, r);
			} else {
				for (int h = 0; h < bbox.length; h += 16) {
					buf16.clear();
					buf16.put(bbox, h, 16);
					buf16.position(0);
					int t = buf16.getInt();
					int l = buf16.getInt();
					int r = buf16.getInt();
					int b = buf16.getInt();
					boundary.update(t, l, b, r);
				}
			}			
		}
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
			qd.missing.clear();
			qd.boundary.clear();
			for(int i = 0; i < qd.ids.size(); i++){
				long id = qd.ids.get(i);
				byte[] bbox = query(id);
				qd.updateBoundary(id, bbox);
			}
			if(qd.boundary.isEmpty()) {
				return null;
			}
			return qd.boundary.getBytes(qd.buf16);
		}
		
		public void put(long id, LatLon ll, QueryData qd) throws Exception {
			qd.buf8.clear();
			qd.buf8.putInt(qd.boundary.convertLat(ll.getLatitude()));
			qd.buf8.putInt(qd.boundary.convertLon(ll.getLongitude()));
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
	
	class SqliteDatabaseAdapter extends DatabaseAdapter {
		private int count;
		private DBDialect sqlite = DBDialect.SQLITE;
		private Connection db;
		private PreparedStatement ps;
		private PreparedStatement queryById;
		private PreparedStatement queryByIdIn10;
		private PreparedStatement queryByIdIn100;
		private boolean createTables;

		public SqliteDatabaseAdapter(File target) {
			super(target);
		}
		

		@Override
		public void prepareToRead() throws Exception {
			db = (Connection) sqlite.getDatabaseConnection(target.getAbsolutePath(), log);
			prepareQueryStats();
			ps = db.prepareStatement("insert or replace into node(id, bbox) values (?, ?)");
			db.setAutoCommit(false);
		}


		private void prepareQueryStats() throws SQLException {
			queryById = db.prepareStatement("select bbox from node where id = ?");
			String b = "select id, bbox from node where id in (";
			String b100 = b;
			String b10 = b;
			for (int i = 0; i < 100; i++) {
				if (i > 0) {
					b100 += ", ";
				}
				b100 += "?";
			}
			for (int i = 0; i < 10; i++) {
				if (i > 0) {
					b10 += ", ";
				}
				b10 += "?";
			}
			queryByIdIn100 = db.prepareStatement(b100 + ")");
			queryByIdIn10 = db.prepareStatement(b10 + ")");
		}
		
		@Override
		public void prepareToCreate() throws DBException, IOException, SQLException {
			if(CREATE) {
				target.delete();
			}
			createTables = !target.exists();
			db = (Connection) sqlite.getDatabaseConnection(target.getAbsolutePath(), log);
			if(createTables) {
				Statement stat = db.createStatement();
				stat.executeUpdate("create table node (id bigint primary key, bbox bytes)"); //$NON-NLS-1$
				stat.executeUpdate("create index IdIndex ON node (id)"); //$NON-NLS-1$
				stat.close();
			}
			prepareQueryStats();
			ps = db.prepareStatement("insert or ignore into node(id, bbox) values (?, ?)");
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
			commitBatch();
			ps.close();
			queryById.close();
			queryByIdIn10.close();
			queryByIdIn100.close();
			db.setAutoCommit(true);
			if(createTables) {
				db.createStatement().execute("VACUUM");
			}
			db.close();
		}
		
		@Override
		public byte[] getBbox(QueryData qd) throws Exception {
			qd.missing.clear();
			qd.boundary.clear();
			int begin = 0;
			while(begin < qd.ids.size()) {
				int ind = 0;
				PreparedStatement p = queryByIdIn100;
				if(qd.ids.size() - begin > 100) {
					while(ind < 100) {
						long l = qd.ids.get(begin + ind);
						queryByIdIn100.setLong(ind + 1, l);
						ind++;
					}
				} else {
					p = queryByIdIn10;
					while(ind < 10) {
						long l = qd.ids.get(begin + ind >= qd.ids.size() ? qd.ids.size() - 1 : begin + ind);
						queryByIdIn10.setLong(ind + 1, l);
						ind++;
					}
				}
				ResultSet rs = p.executeQuery();
				while(rs.next()) {
					byte[] bbox =  rs.getBytes(2);
					long id = rs.getLong(1);
					qd.updateBoundary(id, bbox);
				}
				begin += ind;
			}
			if(qd.boundary.isEmpty()) {
				return null;
			}
			return qd.boundary.getBytes(qd.buf16);
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
	
	protected File[] getSortedFiles(File dir){
		File[] listFiles = dir.listFiles();
		Arrays.sort(listFiles, new Comparator<File>(){
			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return listFiles;
	}
	
	public void splitRegionsOsc(String oscFolder, String indexFile, String planetFile, String ocbfFile) throws Exception {
		File index = new File(indexFile);
		if(!index.exists()) {
			createIdToBBoxIndex(indexFile, planetFile);
		}
		final DatabaseAdapter adapter = new SqliteDatabaseAdapter(index);
		adapter.prepareToRead();
		OsmandRegions regs = new OsmandRegions();
		regs.prepareFile(ocbfFile);
		regs.cacheAllCountries();
		for(File f : getSortedFiles(new File(oscFolder))) {
			if(f.getName().endsWith("osc.gz") && (System.currentTimeMillis() - f.lastModified() > 30000)) {
				updateOsmFile(f, adapter, regs);
			}
		}
		adapter.close();
	}

	private void updateOsmFile(File oscFile, final DatabaseAdapter adapter, OsmandRegions regs) throws FileNotFoundException,
			IOException, SAXException, Exception {
		OsmBaseStorage reader = new OsmBaseStorage();
		final QueryData qd = new QueryData();
		final TLongArrayList included = new TLongArrayList();
		long ms = System.currentTimeMillis();
		reader.getFilters().add(new IOsmStorageFilter() {


			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				try {
					long key = convertId(entityId);
					included.add(key);
					if(entity instanceof Node) {
						LatLon ll = ((Node) entity).getLatLon();
						if (ll != null && ll.getLatitude() != 0 && ll.getLongitude() != 0) {
							qd.ids.clear();
							qd.ids.add(key);
							adapter.getBbox(qd);
							qd.boundary.update(ll);
							qd.written ++;
							adapter.putBbox(key, qd.boundary.getBytes(qd.buf16));
						}
					} else if(entity instanceof Way) {
						adapter.commitBatch();
						qd.ids.clear();
						for(int i = 0; i < ((Way) entity).getNodeIds().size(); i++) {
							qd.ids.add(nodeId(((Way) entity).getNodeIds().get(i)));
						}
						byte[] bbox = adapter.getBbox(qd);
						if(bbox != null) {
							qd.written ++;
							adapter.putBbox(key, bbox);
						}
					} else if (entity instanceof Relation) {
						adapter.commitBatch();
						Iterator<EntityId> iterator = ((Relation) entity).getMemberIds().iterator();
						qd.ids.clear();
						while(iterator.hasNext()) {
							EntityId nid = iterator.next();
							qd.ids.add(convertId(nid));
						}
						byte[] bbox = adapter.getBbox(qd);
						if(bbox != null) {
							qd.written ++;
							adapter.putBbox(key, bbox);
						}
					}
					
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return false;
			}
		});
		
		InputStream stream = new BufferedInputStream(new FileInputStream(oscFile), 8192 * 4);
		InputStream streamFile = stream;
		if (oscFile.getName().endsWith(".bz2")) { //$NON-NLS-1$
			if (stream.read() != 'B' || stream.read() != 'Z') {
//				throw new RuntimeException("The source stream must start with the characters BZ if it is to be read as a BZip2 stream."); //$NON-NLS-1$
			} else {
				stream = new CBZip2InputStream(stream);
			}
		} else if (oscFile.getName().endsWith(".gz")) { //$NON-NLS-1$
			stream = new GZIPInputStream(stream);
		}
		reader.parseOSM(stream, new ConsoleProgressImplementation(), streamFile, false);
		adapter.commitBatch();
		System.out.println("Queried " + (qd.queried * 1000l) / (System.currentTimeMillis() - ms + 1) + " rec/s");
		qd.ids.clear();
		qd.ids.addAll(included);
		ms = System.currentTimeMillis();
		qd.queried = 0;
		adapter.getBbox(qd);
		System.out.println("Final queried " + (qd.queried * 1000l) / (System.currentTimeMillis() - ms + 1) + " rec/s");
		System.out.println(oscFile.getName());
		System.out.println("BBOX " + qd.boundary.depth() + " " + qd.boundary.getBoundaryString());
		Set<String> keyNames = new HashSet<String>();
		Boundary  b = qd.boundary;
		while(b != null) {
			List<BinaryMapDataObject> bbox = regs.queryBbox(
					MapUtils.get31TileNumberX(b.getLeftLon()), 
					MapUtils.get31TileNumberX(b.getRightLon()), 
					MapUtils.get31TileNumberY(b.getTopLat()), 
					MapUtils.get31TileNumberY(b.getBottomLat()));
			for(BinaryMapDataObject bo : bbox) {
				String fn = regs.getFullName(bo);
				String downloadName = regs.getMapDownloadType(fn);
				if(!Algorithms.isEmpty(downloadName)) {
					keyNames.add(downloadName);
				}
			}
			b = b.next;
		}
		System.out.println(keyNames);
		for(String s : keyNames) {
			File folder = new File(oscFile.getParentFile(), s);
			folder.mkdirs();
			File dst = new File(folder, oscFile.getName());
			Algorithms.fileCopy(oscFile, dst);
		}
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
						processor.put( nodeId(entity.getId()), entity.getLatLon(), qd );
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
		String location = "/Users/victorshcherb/osmand/temp/minutes/";
//		String location = "/Users/victorshcherb/osmand/temp/Netherlands-noord-holland.osc";
//		String location = "/Users/victorshcherb/osmand/temp/010.osc.gz";
		
		if (args.length > 1) {
			location = args[1];
		}
		String dbPath = new File(location).getParentFile() + "/bboxbyid.sqlite";
		if (args.length > 2) {
			dbPath = args[2];
		}
		String planetFile = "/Users/victorshcherb/osmand/temp/Netherlands-noord-holland.pbf";
		if (args.length > 3) {
			planetFile = args[3];
		}
		String ocbfFile = "/Users/victorshcherb/osmand/repos/resources/countries-info/regions.ocbf";
		if (args.length > 4) {
			ocbfFile = args[4];
		}
		// create
		if (operation.equals("create")) {
			new IndexIdByBbox().createIdToBBoxIndex(location, dbPath);
		} else if (operation.equals("osc")) {
			new IndexIdByBbox().splitRegionsOsc(location, dbPath, planetFile, ocbfFile);
		} else if (operation.equals("")) {
			IndexIdByBbox ib = new IndexIdByBbox();
			final DatabaseAdapter processor = ib.new SqliteDatabaseAdapter(new File(dbPath));
			processor.prepareToRead();
			QueryData qd = new QueryData();
//			qd.ids.add(relationId(271110));
//			qd.ids.add(wayId(93368155l));
//			qd.ids.add(nodeId(2042972578l));
			processor.getBbox(qd);
			System.out.println("Bbox " + qd.boundary.getTopLat() + ", " + qd.boundary.getLeftLon() + " - " 
					+ qd.boundary.getBottomLat() + ", " + qd.boundary.getRightLon());
			processor.close();
		}
	}
}
