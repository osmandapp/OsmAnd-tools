package net.osmand.render;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.NativeJavaRendering;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class OsmAndTestStyleRenderer {


	private static class OsmAndTestStyleRendererParams {
		double topLat = 0;
		double leftLon = 0;
		double bottomLat = 0;
		double rightLon = 0;
		int zoom = 0;
		int imageHeight = 512; 
		int imageWidth = 512;
		String nativeLib = null;
		String dirWithObf = null;
		File outputDir = null;
		List<String> maps = new ArrayList<String>();
		String[] raster = new String[] {
			"https://tile.osmand.net/hd/{0}/{1}/{2}.png"
		};
 	}

	public static void main(String[] args) throws IOException, XmlPullParserException, SAXException, ParserConfigurationException, TransformerException {
		OsmAndTestStyleRendererParams pms = new OsmAndTestStyleRendererParams();
		for (String a : args) {
			if (a.startsWith("-native=")) {
				pms.nativeLib = a.substring("-native=".length());
			} else if (a.startsWith("-obfFiles=")) {
				pms.dirWithObf = a.substring("-obfFiles=".length());
			} else if (a.startsWith("-bbox=")) {
				// left, top, right, bottom
				String[] bbox = a.substring("-bbox=".length()).split(",");
				pms.leftLon = Double.parseDouble(bbox[0]);
				pms.topLat = Double.parseDouble(bbox[1]);
				pms.rightLon = Double.parseDouble(bbox[2]);
				pms.bottomLat = Double.parseDouble(bbox[3]);
			} else if (a.startsWith("-zoom=")) {
				pms.zoom = Integer.parseInt(a.substring("-zoom=".length()));
			} else if (a.startsWith("-imgh=")) {
				pms.imageHeight = Integer.parseInt(a.substring("-imgh=".length()));
			} else if (a.startsWith("-imgw=")) {
				pms.imageWidth = Integer.parseInt(a.substring("-imgw=".length()));
			} else if (a.startsWith("-output=")) {
				pms.outputDir = new File(a.substring("-output=".length()));
				pms.outputDir.mkdirs();
			} else if (a.startsWith("-maps=")) {
				pms.maps.addAll(Arrays.asList(a.substring("-maps=".length()).split(",")));
			}
		}
		
		if(pms.zoom == 0) {
			System.out.println("Please specify --zoom");
			return;
		}
		if(pms.leftLon == pms.rightLon || pms.topLat == pms.bottomLat) {
			System.out.println("Please specify --bbox in format 'leftlon,toplat,rightlon,bottomlat'");
			return;
		}

//		if (nativeLib != null) {
//			boolean old = NativeLibrary.loadOldLib(nativeLib);
//			NativeLibrary nl = new NativeLibrary();
//			nl.loadFontData(new File(NativeSwingRendering.findFontFolder()));
//			if (!old) {
//				throw new UnsupportedOperationException("Not supported");
//			}
//		}
		
		NativeJavaRendering nsr = pms.nativeLib != null ? NativeJavaRendering.getDefault(pms.nativeLib, null, null) : null;
//		nsr.initFilesInDir(new File(dirWithObf));
		ArrayList<File> obfFiles = new ArrayList<File>();
//		initMaps(dirWithObf, backup, maps, nsr, obfFiles);
		
		int leftx = (int) Math.floor(MapUtils.getTileNumberX(pms.zoom, pms.leftLon));
		int rightx = (int) Math.ceil(MapUtils.getTileNumberX(pms.zoom, pms.rightLon));
		int topY = (int) Math.floor(MapUtils.getTileNumberY(pms.zoom, pms.topLat));
		int bottomY = (int) Math.ceil(MapUtils.getTileNumberY(pms.zoom, pms.bottomLat));
		for (int x = leftx; x <= rightx; x++) {
			for (int y = topY; y <= bottomY; y++) {
				downloadTiles(x, y, pms.zoom, pms);

			}
		}
	}

	private static void downloadTiles(int x, int y, int zoom, OsmAndTestStyleRendererParams pms) throws IOException {
		for (String rasterTemplate : pms.raster) {
			String tileUrl = MessageFormat.format(rasterTemplate, pms.zoom, x, y);
			if (pms.outputDir == null) {
				System.out.printf(MessageFormat.format(rasterTemplate, pms.zoom, x, y));
			} else {
				URL url = new URL(tileUrl);
				URLConnection cn = url.openConnection();
				String ext = tileUrl.substring(tileUrl.lastIndexOf('.')+1);
				File f = new File(pms.outputDir, formatTile(pms.zoom, x, y,ext));
				FileOutputStream fous = new FileOutputStream(f);
				Algorithms.streamCopy(cn.getInputStream(), fous);
				fous.close();
			}
		}
	}

	private static String formatTile(int zoom, int x, int y, String ext) {
		return String.format("%d_%d_%d.%s", zoom, x, y, ext);
	}

	private static void initMaps(File currentDir, String dirWithObf, String backup, String maps, NativeJavaRendering nsr,
			List<File> initFiles) throws IOException {
		for(String map : maps.split(",")) {
			map = map.trim();
			File targetFile = new File(currentDir, map + ".obf");
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


}
