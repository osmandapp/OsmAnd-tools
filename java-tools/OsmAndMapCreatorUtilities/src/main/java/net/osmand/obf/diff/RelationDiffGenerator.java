package net.osmand.obf.diff;


import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.util.Algorithms;
import org.w3c.dom.*;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RelationDiffGenerator {

    private static final String TAG_NODE = "node";
    private static final String TAG_WAY = "way";

    public static void main(String[] args) {

        if(args.length == 1 && args[0].equals("test")) {
            args = new String[4];
            List<String> s = new ArrayList<String>();
            s.add("/Users/macmini/OsmAnd/overpass/test3/23_04_17_20_20_before_rel.osm.gz");
            s.add("/Users/macmini/OsmAnd/overpass/test3/23_04_17_20_20_after_rel.osm");
            s.add("/Users/macmini/OsmAnd/overpass/test3/23_04_17_20_20_after_rel_m.osm.gz");
            //s.add("/Users/macmini/OsmAnd/overpass/test1/23_04_17_19_50_tmp.osm");
            args = s.toArray(new String[0]);
        } else if (args.length < 3) {
            System.out.println("Usage: <path before_rel.osm.gz file> " +
                "<path to after_rel.osm.gz file> " +
                "<path to write result> " +
                "<path to temporary osm file>(optional)");
            System.exit(1);
        }

        String start = args[0];
        String end = args[1];
        String result = args[2];
        File tmpFile = args.length >= 4 ? new File(args[3]) : null;

        try {
            RelationDiffGenerator rdg = new RelationDiffGenerator();
            File endFile = new File(end);
            HashSet<EntityId> nodesWaysFromEnd = rdg.getNodesWaysFromEnd(endFile);
            Document doc = rdg.getDocument(new File(start), tmpFile, nodesWaysFromEnd);
            rdg.mergeDocIntoOsm(doc, endFile, new File(result));
        } catch (IOException | XmlPullParserException | ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    private HashSet<EntityId> getNodesWaysFromEnd(File end) throws XmlPullParserException, IOException {
        InputStream fis;
        if(end.getName().endsWith(".gz")) {
            fis = new GZIPInputStream(new FileInputStream(end));
        } else {
            fis = new FileInputStream(end);
        }

        XmlPullParser parser = PlatformUtil.newXMLPullParser();
        parser.setInput(fis, "UTF-8");
        int tok;
        HashSet<EntityId> result = new HashSet<>();
        while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
            String name = parser.getName();
            if (tok == XmlPullParser.START_TAG ) {
                if (TAG_WAY.equals(name) || TAG_NODE.equals(name)) {
                    Entity.EntityType entityType = TAG_WAY.equals(name) ? Entity.EntityType.WAY : Entity.EntityType.NODE;
                    String id = parser.getAttributeValue("", "id");
                    if (!Algorithms.isEmpty(id)) {
                        EntityId e = new EntityId(entityType, Long.parseLong(id));
                        result.add(e);
                    }
                }
            }
        }
        return result;
    }

    private Document getDocument(File start, @Nullable File tmp, HashSet<EntityId> nodesWaysFromEnd) throws IOException, ParserConfigurationException, XmlPullParserException {
        InputStream fisStart;
        if(start.getName().endsWith(".gz")) {
            fisStart = new GZIPInputStream(new FileInputStream(start));
        } else {
            fisStart = new FileInputStream(start);
        }

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.newDocument();

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
                        if (!nodesWaysFromEnd.contains(e)) {
                            // check members
                            statisticAddedMembers++;
                            write = true;
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

        System.out.println("Processed members. Added node and ways from start:" + statisticAddedMembers);

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
        int length = nodes.getLength();
        for (int i = 0; i < length; i++) {
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
        length = ways.getLength();
        for (int i = 0; i < length; i++) {
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
            destFileSource.delete();
        }

        if (source.getName().endsWith(".gz")) {
            fromFileSource.delete();
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
