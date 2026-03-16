package net.osmand.obf.preparation;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import net.osmand.binary.OsmandOdb;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public final class IndexedStringTableNode {
	private final TreeMap<String, IndexedStringTableNode> subNodes = new TreeMap<>();
	private boolean terminal;
	private final BinaryMapIndexWriter writer;

	public IndexedStringTableNode(BinaryMapIndexWriter writer) {
		this.writer = writer;
	}

	public void addKey(String key, int start) {
		if (start >= key.length()) {
			terminal = true;
			return;
		}
		int end = Math.min(start + 2, key.length());
		String nextKeySuffix = key.substring(start, end);
		IndexedStringTableNode child = subNodes.get(nextKeySuffix);
		if (child == null) {
			child = new IndexedStringTableNode(writer);
			subNodes.put(nextKeySuffix, child);
		}
		child.addKey(key, end);
	}

	private int computeSize() {
		int size = 0;
		for (Map.Entry<String, IndexedStringTableNode> entry : subNodes.entrySet()) {
			String childKey = entry.getKey();
			IndexedStringTableNode child = entry.getValue();
			if (child == null) {
				continue;
			}
			size += CodedOutputStream.computeStringSize(OsmandOdb.IndexedStringTable.KEY_FIELD_NUMBER, childKey);
			if (child.terminal) {
				size += CodedOutputStream.computeTagSize(OsmandOdb.IndexedStringTable.VAL_FIELD_NUMBER);
				size += 4;
			}
			if (!child.subNodes.isEmpty()) {
				int nestedSize = child.computeSize();
				size += CodedOutputStream.computeTagSize(OsmandOdb.IndexedStringTable.SUBTABLES_FIELD_NUMBER);
				size += CodedOutputStream.computeUInt32SizeNoTag(nestedSize);
				size += nestedSize;
			}
		}
		return size;
	}

	public void writeNode(String prefix, Map<String, BinaryFileReference> res, long init) throws IOException {
		CodedOutputStream codedOutStream = writer.getCodedOutStream();
		for (Map.Entry<String, IndexedStringTableNode> entry : subNodes.entrySet()) {
			String subKey = entry.getKey();
			IndexedStringTableNode child = entry.getValue();
			if (child == null) {
				continue;
			}
			String fullKey = prefix + subKey;
			codedOutStream.writeString(OsmandOdb.IndexedStringTable.KEY_FIELD_NUMBER, subKey);
			if (child.terminal) {
				codedOutStream.writeTag(OsmandOdb.IndexedStringTable.VAL_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
				BinaryFileReference ref = BinaryFileReference.createShiftReference(writer.getFilePointer(), init);
				codedOutStream.writeFixed32NoTag(0);
				res.put(fullKey, ref);
			}
			if (!child.subNodes.isEmpty()) {
				codedOutStream.writeTag(OsmandOdb.IndexedStringTable.SUBTABLES_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
				int subtableSize = child.computeSize();
				codedOutStream.writeUInt32NoTag(subtableSize);
				child.writeNode(fullKey, res, init);
			}
		}
	}
}