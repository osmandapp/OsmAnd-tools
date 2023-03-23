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
            args = new String[3];
            List<String> s = new ArrayList<String>();
            s.add("/Users/macmini/OsmAnd/overpass/relation_diff.osm.gz");
            s.add("/Users/macmini/OsmAnd/overpass/relation_start.osm.gz");
            s.add("/Users/macmini/OsmAnd/overpass/relation_end.osm");
            args = s.toArray(new String[0]);
        } else if (args.length < 3) {
            System.out.println("Usage: <path to diff.osm file> <path to relation_start.osm file> " +
                "<path to relation_end.osm file> <path to intermediate result file>(optional, if not set result will write to " + System.getProperty("maps.dir") + "/relation_diff_tmp.osm");
            System.exit(1);
        }

        String diff = args[0];
        String start = args[1];
        String end = args[2];
        String tmp = args.length >= 4 ? args[3] : System.getProperty("maps.dir") + "/relation_diff_tmp.osm";

        try {
            HashSet<LatLon> nodeCoordsCashe = new HashSet<>();
            RelationDiffGenerator rdg = new RelationDiffGenerator();
            HashSet<EntityId> membersDelRelation = rdg.getMembersDeletedRelation(new File(diff), nodeCoordsCashe);
            rdg.mergeStartIntoEnd(new File(start), new File(end), new File(tmp), membersDelRelation, nodeCoordsCashe);
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

    private void mergeStartIntoEnd(File start, File end, File tmp, HashSet<EntityId> members, HashSet<LatLon> nodeCoordsCashe)
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
                            writeMemberElement.setAttribute(parser.getAttributeName(i), parser.getAttributeValue(i));
                        }
                    }
                }
            } else if (tok == XmlPullParser.END_TAG && writeMemberTag.equals(name)) {
                if (writeMemberElement != null) {
                    root.appendChild(writeMemberElement);
                    System.out.println("<" + writeMemberElement.getTagName() + ">");
                }
                writeMemberTag = "";
                writeMemberElement = null;
            }
        }

        System.out.println("Processed members. Size processed:" + statisticAddedMembers + ", total size:" + members.size());

        try {
            OutputStream outputStream = new FileOutputStream(tmp);
            writeXml(doc, outputStream);
        } catch (TransformerException e) {
            e.printStackTrace();
        }
        mergeOsmFiles(tmp, end);
        //tmp.delete();
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

    private void mergeOsmFiles(File tmpSource, File dest) throws IOException {

        boolean gzip = false;
        File tmpUncompressed = null;
        if (dest.getName().endsWith(".gz")) {
            gzip = true;
            tmpUncompressed = new File(dest.getAbsolutePath().replace(".gz", ""));
            try (GZIPInputStream gis = new GZIPInputStream(
                    new FileInputStream(dest));
                 FileOutputStream fos = new FileOutputStream(tmpUncompressed)) {

                // copy GZIPInputStream to FileOutputStream
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
            }
        }

        String startTag = "<osm>";
        String closeTag = "</osm>";

        File toFileSource = gzip ? tmpUncompressed : dest;

        //remove tag </osm>
        RandomAccessFile f = new RandomAccessFile(toFileSource, "rw");
        long length = f.length();
        List<Byte> bytes = new ArrayList<>();
        int max = 0;
        do {
            length--;
            f.seek(length);
            byte b = f.readByte();
            bytes.add(0, b);
            String t = byteListToString(bytes);
            if (closeTag.equals(t)) {
                break;
            }
            max++;
        } while(max < 10);
        f.setLength(length);

        //read file from tag <osm> and write to destination file
        BufferedReader br = new BufferedReader(new FileReader(tmpSource));
        String line;
        boolean startWiting = false;
        while ((line = br.readLine()) != null) {
            if (line.trim().equals(startTag)) {
                startWiting = true;
                continue;
            }
            if (startWiting) {
                line += "\n";
                f.write(line.getBytes());
            }
        }

        f.close();

        if (gzip) {
            FileInputStream fis = new FileInputStream(toFileSource);
            GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(dest));
            Algorithms.streamCopy(fis, gzout);
            fis.close();
            gzout.close();
            tmpUncompressed.delete();
        }
    }

    private String byteListToString(List<Byte> l) {
        if (l == null) {
            return "";
        }
        byte[] array = new byte[l.size()];
        int i = 0;
        for (Byte current : l) {
            array[i] = current;
            i++;
        }
        String s = new String(array, StandardCharsets.UTF_8);
        return s.trim();
    }
}
