package net.osmand.osm.io;


import static net.osmand.osm.io.OsmBaseStorage.ATTR_CHANGESET;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_ID;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_K;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_LAT;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_LON;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_REF;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_ROLE;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_TIMESTAMP;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_TYPE;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_UID;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_USER;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_V;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_VERSION;
import static net.osmand.osm.io.OsmBaseStorage.ATTR_VISIBLE;
import static net.osmand.osm.io.OsmBaseStorage.ELEM_MEMBER;
import static net.osmand.osm.io.OsmBaseStorage.ELEM_ND;
import static net.osmand.osm.io.OsmBaseStorage.ELEM_NODE;
import static net.osmand.osm.io.OsmBaseStorage.ELEM_OSM;
import static net.osmand.osm.io.OsmBaseStorage.ELEM_RELATION;
import static net.osmand.osm.io.OsmBaseStorage.ELEM_TAG;
import static net.osmand.osm.io.OsmBaseStorage.ELEM_WAY;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import gnu.trove.list.array.TLongArrayList;
import net.osmand.data.MapObject;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.EntityInfo;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;

public class OsmStorageWriter {

	private static final String INDENT = "    ";
	private final String INDENT2 = INDENT + INDENT;



	public <T extends Entity> List<T> sort(Collection<T> e) {
		List<T> lst = new ArrayList<T>(e);
		Collections.sort(lst, new Comparator<T>() {

			@Override
			public int compare(T o1, T o2) {
				return Long.compare(o1.getId(), o2.getId());
			}
		});
		return lst;
	}
	
	public void saveStorage(OutputStream output, OsmBaseStorage storage, Collection<EntityId> interestedObjects,
			boolean includeLinks) throws XMLStreamException, IOException {
		Map<EntityId, Entity> entities = storage.getRegisteredEntities();
		Map<EntityId, EntityInfo> entityInfo = storage.getRegisteredEntityInfo();

		saveStorage(output, entities, entityInfo, interestedObjects, includeLinks);
	}

	public void saveStorage(OutputStream output,
			Map<EntityId, Entity> entities, Map<EntityId, EntityInfo> entityInfo,
			Collection<EntityId> interestedObjects, boolean includeLinks)
			throws FactoryConfigurationError, XMLStreamException {
		Set<Node> nodes = new LinkedHashSet<Node>();
		Set<Way> ways = new LinkedHashSet<Way>();
		Set<Relation> relations = new LinkedHashSet<Relation>();
		if (interestedObjects == null) {
			interestedObjects = entities.keySet();
		}
		Stack<EntityId> toResolve = new Stack<EntityId>();
		toResolve.addAll(interestedObjects);
		while (!toResolve.isEmpty()) {
			EntityId l = toResolve.pop();
			if (entities.get(l) instanceof Node) {
				nodes.add((Node) entities.get(l));
			} else if (entities.get(l) instanceof Way) {
				ways.add((Way) entities.get(l));
				if (includeLinks) {
					toResolve.addAll(((Way) entities.get(l)).getEntityIds());
				}
			} else if (entities.get(l) instanceof Relation) {
				relations.add((Relation) entities.get(l));
				if (includeLinks) {
					for(RelationMember rm : ((Relation) entities.get(l)).getMembers()) {
						toResolve.add(rm.getEntityId());
					}
				}
			}
		}
		writeOSM(output, entityInfo, sort(nodes), sort(ways), sort(relations));
	}

	public void writeOSM(OutputStream output, Map<EntityId, EntityInfo> entityInfo, Collection<Node> nodes,
			Collection<Way> ways, Collection<Relation> relations) throws FactoryConfigurationError, XMLStreamException {
		writeOSM(output, entityInfo, nodes, ways, relations, false);
	}
	
	public void writeOSM(OutputStream output, Map<EntityId, EntityInfo> entityInfo, Collection<Node> nodes,
			Collection<Way> ways, Collection<Relation> relations, boolean skipMissingMembers) throws FactoryConfigurationError, XMLStreamException {
		// transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		// String indent = "{http://xml.apache.org/xslt}indent-amount";
		// transformer.setOutputProperty(indent, "4");
		XMLOutputFactory xof = XMLOutputFactory.newInstance();
		XMLStreamWriter streamWriter = xof.createXMLStreamWriter(new OutputStreamWriter(output));
		NumberFormat nf = NumberFormat.getNumberInstance();
		nf.setMaximumFractionDigits(8);
		streamWriter.writeStartDocument();
		Set<EntityId> nd = new HashSet<Entity.EntityId>();
		writeStartElement(streamWriter, ELEM_OSM, "");
		streamWriter.writeAttribute(ATTR_VERSION, "0.6");
		for (Node n : nodes) {
			if(n.getTags().isEmpty()) {
				writeEmptyElement(streamWriter, ELEM_NODE, INDENT);
			} else {
				writeStartElement(streamWriter, ELEM_NODE, INDENT);
			}
			streamWriter.writeAttribute(ATTR_LAT, nf.format(n.getLatitude()));
			streamWriter.writeAttribute(ATTR_LON, nf.format(n.getLongitude()));
			streamWriter.writeAttribute(ATTR_ID, String.valueOf(n.getId()));
			writeEntityAttributes(streamWriter, n, entityInfo.get(EntityId.valueOf(n)));
			if(!n.getTags().isEmpty()) {
				writeTags(streamWriter, n);
				writeEndElement(streamWriter, INDENT);
			}
			if(skipMissingMembers) {
				nd.add(EntityId.valueOf(n));
			}
		}

		for (Way w : ways) {
			writeStartElement(streamWriter, ELEM_WAY, INDENT);
			streamWriter.writeAttribute(ATTR_ID, String.valueOf(w.getId()));
			writeEntityAttributes(streamWriter, w, entityInfo.get(EntityId.valueOf(w)));
			TLongArrayList ids = w.getNodeIds();
			for (int i = 0; i < ids.size(); i++) {
				writeEmptyElement(streamWriter, ELEM_ND, INDENT2);
				streamWriter.writeAttribute(ATTR_REF, String.valueOf(ids.get(i)));
			}
			writeTags(streamWriter, w);
			writeEndElement(streamWriter, INDENT);
			if(skipMissingMembers) {
				nd.add(EntityId.valueOf(w));
			}
		}

		for (Relation r : relations) {
			if(skipMissingMembers) {
				nd.add(EntityId.valueOf(r));
			}
			writeStartElement(streamWriter, ELEM_RELATION, INDENT);
			streamWriter.writeAttribute(ATTR_ID, String.valueOf(r.getId()));
			writeEntityAttributes(streamWriter, r, entityInfo.get(EntityId.valueOf(r)));
			for (RelationMember e : r.getMembers()) {
				if(skipMissingMembers && !nd.contains(e.getEntityId())) {
					continue;
				}
				writeStartElement(streamWriter, ELEM_MEMBER, INDENT2);
				streamWriter.writeAttribute(ATTR_REF, String.valueOf(e.getEntityId().getId()));
				String s = e.getRole();
				if (s == null) {
					s = "";
				}
				streamWriter.writeAttribute(ATTR_ROLE, s);
				streamWriter.writeAttribute(ATTR_TYPE, e.getEntityId().getType().toString().toLowerCase());
				writeEndElement(streamWriter, INDENT2);
			}
			writeTags(streamWriter, r);
			writeEndElement(streamWriter, INDENT);
		}

		writeEndElement(streamWriter, ""); // osm
		streamWriter.writeEndDocument();
		streamWriter.flush();
	}

	private void writeEntityAttributes(XMLStreamWriter writer, Entity i, EntityInfo info) throws XMLStreamException{
		if(i.getId() < 0 && (info == null || info.getAction() == null)){
			writer.writeAttribute("action", "modify");
		}
		if(info != null){
			// for josm editor
			if(info.getAction() != null){
				writer.writeAttribute("action", info.getAction());
			}
			if(info.getChangeset() != null){
				writer.writeAttribute(ATTR_CHANGESET, info.getChangeset());
			}
			if(info.getTimestamp() != null){
				writer.writeAttribute(ATTR_TIMESTAMP, info.getTimestamp());
			}
			if(info.getUid() != null){
				writer.writeAttribute(ATTR_UID, info.getUid());
			}
			if(info.getUser() != null){
				writer.writeAttribute(ATTR_USER, info.getUser());
			}
			if(info.getVisible() != null){
				writer.writeAttribute(ATTR_VISIBLE, info.getVisible());
			}
			if(info.getVersion() != null){
				writer.writeAttribute(ATTR_VERSION, info.getVersion());
			}
		} else {
			// josm compatibility

			writer.writeAttribute(ATTR_VERSION, i.getVersion() == 0 ? "1" : i.getVersion() +"");
		}
	}

	public boolean couldBeWrited(MapObject e){
		if(!Algorithms.isEmpty(e.getName()) && e.getLocation() != null){
			return true;
		}
		return false;
	}


	private void writeStartElement(XMLStreamWriter writer, String name, String indent) throws XMLStreamException{
		writer.writeCharacters("\n"+indent);
		writer.writeStartElement(name);
	}
	
	private void writeEmptyElement(XMLStreamWriter writer, String name, String indent) throws XMLStreamException{
		writer.writeCharacters("\n"+indent);
		writer.writeEmptyElement(name);
	}

	private void writeEndElement(XMLStreamWriter writer, String indent) throws XMLStreamException{
		writer.writeCharacters("\n"+indent);
		writer.writeEndElement();
	}

	private void writeTags(XMLStreamWriter writer, Entity e) throws XMLStreamException{
		for(Entry<String, String> en : e.getTags().entrySet()){
			writeEmptyElement(writer, ELEM_TAG, INDENT2);
			writer.writeAttribute(ATTR_K, replaceInvalid(en.getKey()));
			writer.writeAttribute(ATTR_V, replaceInvalid(en.getValue()));
		}
	}


	private String replaceInvalid(String value) {
		// xdeb2 xd83d xdc86 xdebd
		value = value.replace((char) 0xdd62, ' ').replace((char) 0xd855, ' ');
		return value.replace((char) 0xd83d, ' ').replace((char) 0xdeb2, ' ');

	}
}

