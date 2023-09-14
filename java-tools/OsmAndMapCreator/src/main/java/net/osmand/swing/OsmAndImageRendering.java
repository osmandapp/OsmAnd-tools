package net.osmand.swing;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.FileImageOutputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import net.osmand.NativeJavaRendering;
import net.osmand.NativeJavaRendering.RenderingImageContext;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

public class OsmAndImageRendering {


	private static String[] setupDefaultAttrs(String[] args) {
		if (args.length < 1) {
			args = new String[6];
			args[0] = "-native=/home/victor/projects/osmand/repo/core/binaries/linux/amd64/";
			args[1] = "-obfFiles=/home/victor/projects/osmand/osm-gen";
			args[2] = "-gpxFile=/home/victor/projects/osmand/repo/rendering-tests/VictorTest.gpx";
			args[3] = "-output=/home/victor/projects/osmand/repo/rendering-tests/Victor/";
			args[4] = "-eyepiece=/home/victor/projects/osmand/temp/eyepiece-linux-clang-amd64-release";
			args[5] = "-stylesPath=/home/victor/projects/osmand/repo/resources/rendering_styles/";
		}
		return args;
	}

	static class ImageCombination {
		public ImageCombination(String nm, String zoom, String mapDensity, String rendering, String props) {
			generateName = nm;
			renderingStyle = rendering;
			renderingProperties = props;
			this.zoom = Integer.parseInt(zoom);
			this.mapDensity = Double.parseDouble(mapDensity);
		}
		int zoom;
		double mapDensity;
		String renderingStyle;
		String renderingProperties;
		String generateName;
	}

	public static class HTMLContent {

		private File file;
		private List<String> descriptions = new ArrayList<String>();
		private List<List<String>> files = new ArrayList<List<String>>();
		private String header;
		private String footer;
		private String rowheader;
		private String rowfooter;
		private String rowContent;
		private String webSiteUrl;


		public HTMLContent(File file, String webSite) {
			this.file = file;
			this.webSiteUrl = webSite;
		}

		public void addFile(String file) {
			files.get(files.size() - 1).add(file);
		}

		public void newRow(String description) {
			descriptions.add(description);
			files.add(new ArrayList<String>());
		}

		public void write() throws IOException {
			BufferedWriter w = new BufferedWriter(new FileWriter(file));
			w.write(header);
			for(int i = 0; i< descriptions.size();i++){
				String desc = descriptions.get(i);
				List<String> fs = files.get(i);
				w.write(rowheader.replace("$DESCRIPTION", desc));
				for(String f : fs) {
					w.write(rowContent.replace("$FILENAME", f).replace("$PWD", webSiteUrl));
				}
				w.write(rowfooter.replace("$DESCRIPTION", desc));
			}
			w.write(footer);
			w.close();
		}

		public void setContent(String textContent) {
			System.out.println("Generate html page " + textContent);
			int rw = textContent.indexOf("<ROW>");
			int erw = textContent.indexOf("</ROW>");
			header = textContent.substring(0, rw);
			String row = textContent.substring(rw + "<ROW>".length(), erw);
			footer = textContent.substring(erw + "</ROW>".length());

			int crw = row.indexOf("<ROW_ELEMENT>");
			int cerw = row.indexOf("</ROW_ELEMENT>");
			rowheader = row.substring(0, crw);
			rowContent = row.substring(crw + "<ROW_ELEMENT>".length(), cerw);
			rowfooter = row.substring(cerw + "</ROW_ELEMENT>".length());

		}

	}

	public static void main(String[] args) throws IOException, XmlPullParserException, SAXException, ParserConfigurationException, TransformerException {
		String nativeLib = null;
		String eyepiece = null;
		String dirWithObf = null;
		String websiteUrl = "";
		String gpxFile = null;
		String outputFiles = null;
		String stylesPath = null;
		args = setupDefaultAttrs(args);
		for (String a : args) {
			if (a.startsWith("-native=")) {
				nativeLib = a.substring("-native=".length());
			} else if (a.startsWith("-websiteUrl=")) {
				// https://jenkins.osmand.net/job/OsmAndTestRendering/ws/rendering-tests/
				websiteUrl = a.substring("-websiteUrl=".length());
			} else if (a.startsWith("-obfFiles=")) {
				dirWithObf = a.substring("-obfFiles=".length());
			} else if (a.startsWith("-gpxFile=")) {
				gpxFile = a.substring("-gpxFile=".length());
			} else if (a.startsWith("-output=")) {
				outputFiles = a.substring("-output=".length());
			} else if (a.startsWith("-eyepiece=")) {
				eyepiece = a.substring("-eyepiece=".length());
			} else if (a.startsWith("-stylesPath=")) {
				stylesPath = a.substring("-stylesPath=".length());
			}
		}

		String backup = null;
//		if (nativeLib != null) {
//			boolean old = NativeLibrary.loadOldLib(nativeLib);
//			NativeLibrary nl = new NativeLibrary();
//			nl.loadFontData(new File(NativeSwingRendering.findFontFolder()));
//			if (!old) {
//				throw new UnsupportedOperationException("Not supported");
//			}
//		}
		DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		org.w3c.dom.Document doc = docBuilder.parse(new File(gpxFile));
		Element de = doc.getDocumentElement();
		String defZoom = getAttribute(de, "zoom", "11");
		String defMapDensity = getAttribute(de, "mapDensity", "1");
		String defRenderingName = getAttribute(de, "renderingName", "default");
		String defRenderingProps = getAttribute(de, "renderingProperties", "");
		String defWidth = getAttribute(de, "width", "1366");
		String defHeight = getAttribute(de, "height", "768");

		NodeList nl = doc.getElementsByTagName("wpt");
		NodeList gen = doc.getElementsByTagName("html");
		File gpx = new File(gpxFile);
		HTMLContent html = null;

		if (gen.getLength() > 0) {
			String gpxName = gpx.getName().substring(0, gpx.getName().length() - 4);
			String outputFName = gpxName;
			if(eyepiece != null) {
			//	outputFName += "_eye";
			}
			html = new HTMLContent(new File(gpx.getParentFile(), outputFName  + ".html"), websiteUrl+ gpxName);
			StringWriter sw = new StringWriter();
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
			transformer.transform(new DOMSource(gen.item(0)), new StreamResult(sw));
			html.setContent(sw.toString());
		}
		for (int i = 0; i < nl.getLength(); i++) {
			Element e = (Element) nl.item(i);
			double lat = Double.parseDouble(e.getAttribute("lat"));
			double lon = Double.parseDouble(e.getAttribute("lon"));

			int imageHeight = Integer.parseInt(getSubAttr(e, "height", defHeight));
			int imageWidth = Integer.parseInt(getSubAttr(e, "width", defWidth));
			String name = getSubAttr(e, "name", (lat+"!"+lon).replace('.', '_'));
			String maps = getSubAttr(e, "maps", "") ;

			String mapDensities = getSubAttr(e, "mapDensity", defMapDensity);
			String zooms = getSubAttr(e, "zoom", defZoom);
			String renderingNames = getSubAttr(e, "renderingName", defRenderingName) ;
			String renderingProperties = getSubAttr(e, "renderingProperties", defRenderingProps) ;
			if(maps.isEmpty()) {
                throw new UnsupportedOperationException("No maps element found for wpt "+ name);
            }
			NativeJavaRendering nsr = nativeLib != null ? NativeJavaRendering.getDefault(
					nativeLib, null, NativeSwingRendering.findFontFolder()) : null;
//			nsr.initFilesInDir(new File(dirWithObf));
			ArrayList<File> obfFiles = new ArrayList<File>();
			initMaps(dirWithObf, backup, gpxFile, maps, nsr, obfFiles);
			List<ImageCombination> ls = getCombinations(name, zooms, mapDensities, renderingNames, renderingProperties) ;
			if(html != null) {
				html.newRow(getSubAttr(e, "desc", ""));
			}
			for (ImageCombination ic : ls) {
				System.out.println("Generate " + ic.generateName + " style " + ic.renderingStyle);
				if (eyepiece != null) {
					try {
						String line = eyepiece;
						double dx = ic.mapDensity;
						line += " -latLon=" + lat+";"+lon;
						line += " -stylesPath=\""+stylesPath+"\"";
						String styleName = ic.renderingStyle;
						if(styleName.endsWith(".render.xml")) {
							styleName = styleName.substring(0, styleName.length() - ".render.xml".length());
						}
						line += " -styleName="+styleName;
						for(File f : obfFiles) {
							line += " -obfFile=\""+f.getAbsolutePath()+"\"";
						}
						String[] listProps = ic.renderingProperties.split(",");
						for(String p : listProps) {
							if(p.trim().length() > 0) {
								line += " -styleSetting:"+p;
							}
						}
//						line += " -useLegacyContext";
						line += " -outputImageWidth=" + imageWidth;
						line += " -outputImageHeight=" + imageHeight;
						final String fileName = ic.generateName + ".png";
						line += " -outputImageFilename=\"" + outputFiles + "/" + fileName+"\"";
						line += " -referenceTileSize="+(int)(dx*256);
						line += " -displayDensityFactor="+dx;
						line += " -zoom="+ic.zoom;
						line += " -outputImageFormat=png";
						line += " -verbose";
						System.out.println(line);
						Process p = Runtime.getRuntime().exec(line);
						BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
						while ((line = input.readLine()) != null) {
							System.out.println(line);
						}
						input.close();
						if (html != null) {
							html.addFile(fileName);
						}
					} catch (Exception err) {
						err.printStackTrace();
					}

				}
				if (nativeLib != null) {
					final String fileName = ic.generateName + ".png";
					System.out.println("Generate to " + fileName);
					nsr.loadRuleStorage(ic.renderingStyle, ic.renderingProperties);
					BufferedImage mg = nsr.renderImage(
							new RenderingImageContext(lat, lon, imageWidth, imageHeight, ic.zoom, ic.mapDensity));

					ImageWriter writer = ImageIO.getImageWritersBySuffix("png").next();

					if (html != null) {
						html.addFile(fileName);
					}
					writer.setOutput(new FileImageOutputStream(new File(outputFiles, fileName)));
					writer.write(mg);
				}
			}
		}
		if(html != null) {
			html.write();
		}
	}

	private static List<ImageCombination> getCombinations(String name, String zooms, String mapDensities,
			String renderingNames, String renderingProperties) {
		String[] zoomsStr = zooms.split(",");
		String[] mapDenses = mapDensities.split(",");
		String[] renderingNamesStr = renderingNames.split(",");
		String[] renderingPropertiesStr = renderingProperties.split(";");
		List<ImageCombination> list = new ArrayList<OsmAndImageRendering.ImageCombination>();

		for (int i1 = 0; i1 < zoomsStr.length; i1++) {
			String name1 = append(name, "z", zoomsStr, i1);
			for (int i2 = 0; i2 < mapDenses.length; i2++) {
				String name2 = append(name1, "mapDens", mapDenses, i2);
				for (int i3 = 0; i3 < renderingNamesStr.length; i3++) {
					String name3 = append(name2, "", renderingNamesStr, i3);
					for (int i4 = 0; i4 < renderingPropertiesStr.length; i4++) {
						String name4 = append(name3, "", renderingPropertiesStr, i4);
						String rst = renderingPropertiesStr[i4];
						if(!rst.contains("noPolygons")) {
							if(rst.trim().length() > 0) {
								rst += ",";
							}
							rst += "noPolygons=false";
						}
						list.add(new ImageCombination(name4, zoomsStr[i1], mapDenses[i2],
								renderingNamesStr[i3] + ".render.xml", rst));

					}
				}
			}
		}
		return list;
	}

	private static String append(String name, String key, String[] ar, int ind) {
		if (ar.length == 1) {
			return name;
		}
		String vl = ar[ind];
//		if (vl.length() > 10) {
//			vl = vl.substring(0, 10);
//		}
		String r = "";
		if (key.length() > 0) {
			r += key + "_";
		}
		r +=  vl.replace('=', '_').replace(',', '-');
		if(r.length() > 0){
			return name + "-" + r;
		}
		return name;
	}

	private static void initMaps(String dirWithObf, String backup, String gpxFile, String maps, NativeJavaRendering nsr,
			List<File> initFiles) throws IOException {
		for(String map : maps.split(",")) {
			map = map.trim();
			File targetFile = new File(new File(gpxFile).getParentFile(), map + ".obf");
			File sourceFile = new File(dirWithObf + "/" + map + ".obf");
			File sourceZip = new File(dirWithObf + "/" + map + ".obf.zip");
			if (backup != null) {
				if (!sourceZip.exists() && !sourceFile.exists()) {
					sourceZip = new File(backup + "/" + map + ".obf.zip");
				}
				if (!sourceFile.exists()) {
					sourceFile = new File(backup + "/" + map + ".obf");
				}
			}
			if(sourceFile.exists()) {
				targetFile = sourceFile;
			} else if(!sourceFile.exists() && !sourceZip.exists() && !targetFile.exists()){
				throw new IllegalStateException("File "+sourceFile.getAbsolutePath()+ " is not found");
			} else if(sourceZip.exists()) {
				if(!targetFile.exists() || (targetFile.lastModified() != sourceZip.lastModified())) {
					ZipInputStream zipIn = new ZipInputStream(new FileInputStream(sourceZip));
					ZipEntry entry = null;
					while((entry = zipIn.getNextEntry()) != null) {
						byte[] bs = new byte[1024];
						if(entry.getName().endsWith(".obf")) {
							FileOutputStream fout = new FileOutputStream(targetFile);
							int l = 0;
							while ((l = zipIn.read(bs)) >= 0) {
								fout.write(bs, 0, l);
							}
							fout.close();
							break;
						}
					}
					zipIn.close();

					targetFile.setLastModified(sourceZip.lastModified());
				}
			}
			if (nsr != null) {
				nsr.initMapFile(targetFile.getAbsolutePath(), true);
			}
			initFiles.add(targetFile);
		}
	}

	private static String getSubAttr(Element e, String attrName, String def) {
		NodeList ns = e.getElementsByTagName(attrName);
		if(ns.getLength() > 0) {
			String tt = ns.item(0).getTextContent();
			if(tt != null && tt.length() > 0) {
				return tt;
			}
		}
		return def;
	}

	private static String getAttribute(Element e, String attrName, String def) {
		String vl = e.getAttribute(attrName);
		if(vl != null && vl.length() > 0) {
			return vl;
		}
		return def;
	}

}
