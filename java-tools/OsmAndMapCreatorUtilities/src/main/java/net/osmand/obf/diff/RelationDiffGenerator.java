package net.osmand.obf.diff;


import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.util.Algorithms;
import org.w3c.dom.*;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RelationDiffGenerator {

    private static final String TAG_ACTION = "action";
    private static final String TAG_OLD_END = "old";
    private static final String TAG_MEMBER = "member";
    private static final String ATTRIBUTE_TYPE = "type";
    private static final String ATTRIBUTE_REF = "ref";
    private static final String TYPE_DELETE = "delete";
    private static final String TAG_RELATION = "relation";
    private static final String TAG_NODE = "node";
    private static final String TAG_WAY = "way";
    private static final String TAG_NODE_OF_WAY = "nd";

    public static void main(String[] args) {

        if(args.length == 1 && args[0].equals("test")) {
            args = new String[5];
            List<String> s = new ArrayList<String>();
            s.add("/Users/macmini/OsmAnd/overpass/test1/23_03_19_24_00_diff.osm.gz");
            s.add("/Users/macmini/OsmAnd/overpass/test1/23_03_19_24_00_start_test.osm.gz");
            s.add("/Users/macmini/OsmAnd/overpass/test1/23_03_19_24_00_end_test.osm.gz");
            s.add("/Users/macmini/OsmAnd/overpass/test1/23_03_19_24_00_result.osm.gz");
            s.add("/Users/macmini/OsmAnd/overpass/test1/23_03_19_24_00_tmp.osm");
            args = s.toArray(new String[0]);
        } else if (args.length < 3) {
            System.out.println("Usage: <path to diff osm.gz file> <path start osm.gz file> " +
                "<path to end osm.gz file> " +
                "<path to write osm.gz result> " +
                "<path to temporary osm file>(optional)");
            System.exit(1);
        }

        String diff = args[0];
        String start = args[1];
        String end = args[2];
        String result = args[3];
        File tmpFile = args.length >= 5 ? new File(args[4]) : null;

        try {
            HashSet<LatLon> nodeCoordsCashe = new HashSet<>();
            RelationDiffGenerator rdg = new RelationDiffGenerator();
            HashSet<EntityId> membersDelRelation = rdg.getMembersDeletedRelation(new File(diff), nodeCoordsCashe);
            File endFile = new File(end);
            Document doc = rdg.getDocument(new File(start), endFile, tmpFile, membersDelRelation, nodeCoordsCashe);
            rdg.mergeDocIntoOsm(doc, endFile, new File(result));
        } catch (IOException | XmlPullParserException | SAXException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    private HashSet<EntityId> getMembersDeletedRelation(File diff, HashSet<LatLon> nodeCoordsCashe) throws IOException, XmlPullParserException {
        InputStream fis;
        if(diff.getName().endsWith(".gz")) {
            fis = new GZIPInputStream(new FileInputStream(diff));
        } else {
            fis = new FileInputStream(diff);
        }

        XmlPullParser parser = PlatformUtil.newXMLPullParser();
        parser.setInput(fis, "UTF-8");
        int tok;
        boolean actionDelete = false;
        boolean relationDelete = false;
        HashSet<EntityId> result = new HashSet<>();
        while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
            if (tok == XmlPullParser.START_TAG ) {
                String name = parser.getName();

                if (relationDelete) {
                    if (TAG_MEMBER.equals(name)) {
                        String type = parser.getAttributeValue("", ATTRIBUTE_TYPE);
                        String ref = parser.getAttributeValue("", ATTRIBUTE_REF);
                        if (!Algorithms.isEmpty(type) && !Algorithms.isEmpty(ref)) {
                            if (TAG_NODE.equals(type)) {
                                EntityId e = new EntityId(Entity.EntityType.NODE, Long.parseLong(ref));
                                result.add(e);
                            } else if (TAG_WAY.equals(type)) {
                                EntityId e = new EntityId(Entity.EntityType.WAY, Long.parseLong(ref));
                                result.add(e);
                            }
                        }
                    }
                    if (TAG_NODE_OF_WAY.equals(name)) {
                        String lat = parser.getAttributeValue("", "lat");
                        String lon = parser.getAttributeValue("", "lon");
                        if (!Algorithms.isEmpty(lat) && !Algorithms.isEmpty(lon)) {
                            nodeCoordsCashe.add(new LatLon(Float.parseFloat(lat), Float.parseFloat(lon)));
                        }
                    }
                }

                if (actionDelete && TAG_RELATION.equals(name)) {
                    relationDelete = true;
                }

                // TODO perhaps add cache of deleted nodes and ways for remove them from result and nodeCoordsCashe

                if (TAG_ACTION.equals(name)
                        && ATTRIBUTE_TYPE.equals(parser.getAttributeName(0))
                        && TYPE_DELETE.equals(parser.getAttributeValue(0))) {
                    actionDelete = true;
                }
            } else if (tok == XmlPullParser.END_TAG) {
                String name = parser.getName();
                if (TAG_OLD_END.equals(name)) {
                    actionDelete = false;
                    relationDelete = false;
                }
            }
        }
        return result;
    }

    private Document getDocument(File start, File end, @Nullable File tmp, HashSet<EntityId> members, HashSet<LatLon> nodeCoordsCashe)
            throws IOException, ParserConfigurationException, SAXException, XmlPullParserException {

        InputStream fisStart;
        if(start.getName().endsWith(".gz")) {
            fisStart = new GZIPInputStream(new FileInputStream(start));
        } else {
            fisStart = new FileInputStream(start);
        }

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.newDocument();

        int cnt = members.size();
        //TODO add also for check nodeCoordsCashe
        removeRepeatedMembers(end, members);
        System.out.println("Clear members. Size before:" + cnt + ", after:" + members.size());

        //parse start file and put data to doc
        Element root = doc.createElement("osm");
        doc.appendChild(root);
        XmlPullParser parser = PlatformUtil.newXMLPullParser();
        parser.setInput(fisStart, "UTF-8");
        int tok;
        String writeMemberTag = "";
        Element writeMemberElement = null;
        int statisticAddedMembers = 0;
        while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
            String name = parser.getName();
            if (tok == XmlPullParser.START_TAG ) {

                if (writeMemberElement != null) {
                    Element child = doc.createElement(name);
                    int c = parser.getAttributeCount();
                    for (int i = 0; i < c; i++) {
                        child.setAttribute(parser.getAttributeName(i), parser.getAttributeValue(i));
                    }
                    writeMemberElement.appendChild(child);
                }

                if (TAG_WAY.equals(name) || TAG_NODE.equals(name)) {
                    Entity.EntityType type = TAG_WAY.equals(name) ? Entity.EntityType.WAY : Entity.EntityType.NODE;
                    boolean write = false;
                    String id = parser.getAttributeValue("", "id");
                    if (!Algorithms.isEmpty(id)) {
                        EntityId e = new EntityId(type, Long.parseLong(id));
                        if (members.contains(e)) {
                            // check members
                            write = true;
                            statisticAddedMembers++;
                        } else if (type == Entity.EntityType.NODE) {
                            // check node coords cache
                            String lat = parser.getAttributeValue("", "lat");
                            String lon = parser.getAttributeValue("", "lon");
                            if (!Algorithms.isEmpty(lat) && !Algorithms.isEmpty(lon)) {
                                LatLon latLon = new LatLon(Float.parseFloat(lat), Float.parseFloat(lon));
                                if (nodeCoordsCashe.contains(latLon)) {
                                    write = true;
                                }
                            }
                        }
                    }
                    if (write) {
                        writeMemberTag = name;
                        writeMemberElement = doc.createElement(writeMemberTag);
                        int c = parser.getAttributeCount();
                        for (int i = 0; i < c; i++) {
                            if (parser.getAttributeName(i).equals("changeset")) {
                                continue;
                            }
                            writeMemberElement.setAttribute(parser.getAttributeName(i), parser.getAttributeValue(i));
                        }
                    }
                }
            } else if (tok == XmlPullParser.END_TAG && writeMemberTag.equals(name)) {
                if (writeMemberElement != null) {
                    root.appendChild(writeMemberElement);
                }
                writeMemberTag = "";
                writeMemberElement = null;
            }
        }

        System.out.println("Processed members. Size processed:" + statisticAddedMembers + ", total size:" + members.size());

        if (tmp != null) {
            try {
                OutputStream outputStream = new FileOutputStream(tmp);
                writeXml(doc, outputStream);
            } catch (TransformerException e) {
                e.printStackTrace();
            }
        }

        return doc;
    }

    private void removeRepeatedMembers(File endFile, HashSet<EntityId> members) throws IOException, XmlPullParserException {
        InputStream fis;
        if(endFile.getName().endsWith(".gz")) {
            fis = new GZIPInputStream(new FileInputStream(endFile));
        } else {
            fis = new FileInputStream(endFile);
        }

        XmlPullParser parser = PlatformUtil.newXMLPullParser();
        parser.setInput(fis, "UTF-8");
        int tok;
        HashSet<EntityId> result = new HashSet<>();
        while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
            if (tok == XmlPullParser.START_TAG) {
                String name = parser.getName();
                if (TAG_WAY.equals(name) || TAG_NODE.equals(name)) {
                    Entity.EntityType type = TAG_WAY.equals(name) ? Entity.EntityType.WAY : Entity.EntityType.NODE;
                    String id = parser.getAttributeValue("", "id");
                    EntityId e = new EntityId(type, Long.parseLong(id));
                    members.remove(e);
                }
            }
        }
    }

    // write doc to output stream
    private void writeXml(Document doc,
                                 OutputStream output)
            throws TransformerException {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        DOMSource source = new DOMSource(doc);
        StreamResult streamResult = new StreamResult(output);
        transformer.transform(source, streamResult);
    }

    private void mergeDocIntoOsm(Document doc, File source, File dest) throws IOException {
        boolean gzip = false;
        File sourceUncompressed = null;
        if (source.getName().endsWith(".gz")) {
            gzip = true;
            sourceUncompressed = new File(source.getAbsolutePath().replace(".gz", ""));
            try (GZIPInputStream gis = new GZIPInputStream(
                    new FileInputStream(source));
                 FileOutputStream fos = new FileOutputStream(sourceUncompressed)) {

                // copy GZIPInputStream to FileOutputStream
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
            }
        }

        File fromFileSource = gzip ? sourceUncompressed : source;
        File destFileSource = dest.getName().endsWith(".gz") ? new File(dest.getAbsolutePath().replace(".gz", "")) : dest;
        destFileSource.delete();

        BufferedReader sourceFile = new BufferedReader(new FileReader(fromFileSource));
        RandomAccessFile destFile = new RandomAccessFile(destFileSource, "rw");

        // write sorted nodes
        NodeList nodes = doc.getElementsByTagName("node");
        String line = null;
        boolean isWayStarted = false;
        boolean isEndOfFile = false;
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            NamedNodeMap atrs = n.getAttributes();
            Long idNode = -1L;
            if (atrs != null) {
                Node idAttr = atrs.getNamedItem("id");
                if (idAttr != null) {
                    idNode = Long.parseLong(idAttr.getNodeValue());
                }
            }
            if (idNode > 0) {

                if (isWayStarted || isEndOfFile) {
                    // if way was started or reached end of file just write nodes from doc
                    String ns = nodeToString(n);
                    destFile.write(ns.getBytes());
                    continue;
                }

                if (line != null && line.trim().startsWith("<node")) {
                    // sort (writing) nodes until idNode is bigger
                    Long id = getIdFromString(line);
                    if (idNode < id) {
                        String ns = nodeToString(n);
                        destFile.write(ns.getBytes());
                        continue;
                    } else {
                        line += "\n";
                        destFile.write(line.getBytes());
                        line = null;
                    }
                }

                while ((line = sourceFile.readLine()) != null) {
                    if (line.contains("</osm>") || line.contains("<relation")) {
                        isEndOfFile = true;
                        String ns = nodeToString(n);
                        destFile.write(ns.getBytes());
                        break;
                    }
                    if (line.trim().startsWith("<way")) {
                        isWayStarted = true;
                        String ns = nodeToString(n);
                        destFile.write(ns.getBytes());
                        break;
                    }
                    if (line.trim().startsWith("<node")) {
                        Long id = getIdFromString(line);
                        if (idNode.equals(id)) {
                            line += "\n";
                            destFile.write(line.getBytes());
                            //avoid duplicate
                            line = null;
                            break;
                        } else if (idNode < id) {
                            String ns = nodeToString(n);
                            destFile.write(ns.getBytes());
                            //stop reading and sort other nodes from doc above
                            break;
                        } else {
                            line += "\n";
                            destFile.write(line.getBytes());
                        }
                    } else {
                        line += "\n";
                        destFile.write(line.getBytes());
                    }
                }
            }
        }

        if (line != null && line.trim().startsWith("<node")) {
            line += "\n";
            destFile.write(line.getBytes());
        }

        // write sorted ways
        NodeList ways = doc.getElementsByTagName("way");
        for (int i = 0; i < ways.getLength(); i++) {
            Node w = ways.item(i);
            NamedNodeMap atrs = w.getAttributes();
            Long idWay = -1L;
            if (atrs != null) {
                Node idAttr = atrs.getNamedItem("id");
                if (idAttr != null) {
                    idWay = Long.parseLong(idAttr.getNodeValue());
                }
            }
            if (idWay > 0) {

                if (isEndOfFile) {
                    // if reached end of file just write ways from doc
                    String ns = nodeToString(w);
                    destFile.write(ns.getBytes());
                    continue;
                }

                if (line != null && line.trim().startsWith("<way")) {
                    // sort (writing) ways until idWay is bigger
                    Long id = getIdFromString(line);
                    if (idWay < id) {
                        String ns = nodeToString(w);
                        destFile.write(ns.getBytes());
                        continue;
                    } else {
                        line += "\n";
                        destFile.write(line.getBytes());
                        line = null;
                    }
                }

                while ((line = sourceFile.readLine()) != null) {
                    if (line.contains("</osm>") || line.contains("<relation")) {
                        isEndOfFile = true;
                        String ns = nodeToString(w);
                        destFile.write(ns.getBytes());
                        break;
                    }
                    if (line.trim().startsWith("<way")) {
                        Long id = getIdFromString(line);
                        if (idWay.equals(id)) {
                            line += "\n";
                            destFile.write(line.getBytes());
                            //avoid duplicate
                            line = null;
                            break;
                        } else if (idWay < id) {
                            String ns = nodeToString(w);
                            destFile.write(ns.getBytes());
                            //stop reading and sort other ways from doc above
                            break;
                        } else {
                            line += "\n";
                            destFile.write(line.getBytes());
                        }
                    } else {
                        line += "\n";
                        destFile.write(line.getBytes());
                    }
                }
            }
        }

        if (line != null && line.trim().startsWith("<way")) {
            line += "\n";
            destFile.write(line.getBytes());
        } else if (isEndOfFile) {
            line += "\n";
            destFile.write(line.getBytes());
        }

        // write remained part of file
        while ((line = sourceFile.readLine()) != null) {
            line += "\n";
            destFile.write(line.getBytes());
        }


        if (dest.getName().endsWith(".gz")) {
            FileInputStream fis = new FileInputStream(destFileSource);
            GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(dest));
            Algorithms.streamCopy(fis, gzout);
            fis.close();
            gzout.close();
            // TODO uncomment after test
            //destFileSource.delete();
        }

        destFile.close();
        sourceFile.close();
    }

    private Long getIdFromString(String line) {
        Long id = -1L;
        String search = " id=\"";
        if (line.contains(search)) {
            String m = line.substring(line.indexOf(search) + search.length());
            int i = m.indexOf("\"");
            m = m.substring(0, i);
            try {
                id = Long.parseLong(m);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
        return id;
    }

    private String nodeToString(Node node) {
        StringWriter sw = new StringWriter();
        try {
            Transformer t = TransformerFactory.newInstance().newTransformer();
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            t.transform(new DOMSource(node), new StreamResult(sw));
        } catch (TransformerException te) {
            System.out.println("nodeToString Transformer Exception");
        }
        String r = sw.toString();
        return r.replaceAll("<", "  <");
    }
}
