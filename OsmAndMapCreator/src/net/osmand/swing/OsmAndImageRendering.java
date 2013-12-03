package net.osmand.swing;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.FileImageOutputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import net.osmand.NativeLibrary;
import net.osmand.swing.NativeSwingRendering.RenderingImageContext;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

public class OsmAndImageRendering {
	
	
	public static void main(String[] args) throws IOException, XmlPullParserException, SAXException, ParserConfigurationException {
		String nativeLib = "/home/victor/projects/osmand/repo/core/binaries/linux/amd64/";
		String dirWithObf = "/home/victor/projects/osmand/osm-gen";
		String outputFiles = "/home/victor/projects/osmand/repo/rendering-tests/Publish/";
		String gpxFile = "/home/victor/projects/osmand/repo/rendering-tests/Publish.gpx";
		
//		String nativeLib = args[0];
//		String dirWithObf = args[1];
//		String gpxFile = args[2];
//		String outputFiles = args[3];
		boolean old = NativeLibrary.loadOldLib(nativeLib);
		if(!old) {
			throw new UnsupportedOperationException("Not supported"); 
		}
		DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		org.w3c.dom.Document doc = docBuilder.parse(new File(gpxFile));
		NodeList nl = doc.getElementsByTagName("wpt");
		
		for (int i = 0; i < nl.getLength(); i++) {
			Element e = (Element) nl.item(i);
			double lat = Double.parseDouble(e.getAttribute("lat"));
			double lon = Double.parseDouble(e.getAttribute("lon"));
			double zoomScale = Double.parseDouble(getAttr(e, "zoomScale", "0"));
			int zoom = Integer.parseInt(getAttr(e, "zoom", "11"));
			int imageHeight = Integer.parseInt(getAttr(e, "height", "768"));
			int imageWidth = Integer.parseInt(getAttr(e, "width", "1366"));
			String name = getAttr(e, "name", (lat+"!"+lon).replace('.', '_'));
			String renderingName = getAttr(e, "renderingName", "default") + ".render.xml";
			String rprops = getAttr(e, "renderingProperties", "") ;
			String maps = getAttr(e, "maps", "") ;
			if(maps.isEmpty()) {
				throw new UnsupportedOperationException("No maps element found for wpt "+ name);
			}
			NativeSwingRendering nsr = new NativeSwingRendering(false);
//			nsr.initFilesInDir(new File(dirWithObf));
			for(String map : maps.split(",")) {
				File f = new File(dirWithObf + "/" + map+".obf");
				File fzip = new File(dirWithObf + "/" + map+".obf.zip");
				if(!f.exists() && !fzip.exists()){
					throw new IllegalStateException("File "+f.getAbsolutePath()+ " is not found");
				} else if(!f.exists()) {
					f = new File(new File(gpxFile).getParentFile(), f.getName());
					if(!f.exists() || (f.lastModified() != fzip.lastModified())) {
						ZipInputStream zipIn = new ZipInputStream(new FileInputStream(fzip));
						ZipEntry entry = null;
						while((entry = zipIn.getNextEntry()) != null) {
							byte[] bs = new byte[1024];
							if(entry.getName().endsWith(".obf")) {
								FileOutputStream fout = new FileOutputStream(f);
								int l = 0;
								while ((l = zipIn.read(bs)) >= 0) {
									fout.write(bs, 0, l);
								}
								fout.close();
								break;
							}
						}
						zipIn.close();
							
						f.setLastModified(fzip.lastModified());
					}
				}
				nsr.initMapFile(f.getAbsolutePath());
			}
			nsr.loadRuleStorage(renderingName, rprops);

			BufferedImage mg = nsr.renderImage(new RenderingImageContext(lat, lon, imageWidth, imageHeight, zoom,
					zoomScale));
			ImageWriter writer = ImageIO.getImageWritersBySuffix("png").next();
			writer.setOutput(new FileImageOutputStream(new File(outputFiles,  name+".png")));
			writer.write(mg);
		}
	}
	
	private static String getAttr(Element e, String attrName, String def) {
		NodeList ns = e.getElementsByTagName(attrName);
		if(ns.getLength() > 0) {
			String tt = ns.item(0).getTextContent();
			if(tt != null && tt.length() > 0) {
				return tt;
			}
		}
		return def;
	}
	
}
