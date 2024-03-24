package net.osmand.server.api.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.Gson;

import net.osmand.util.Algorithms;

// TODO cache json & recalculate
// TODO publish button 
@Service
public class PluginsService {
	
	private static final String INFO_JSON = "info.json";
	private static final String ITEMS_JSON = "items.json";
	private static final String UPLOADS_PLUGINS = "uploads/plugins";
	private static final String NIGHTLY = "nightly";
	
	@Value("${osmand.files.location}")
    private String pathToRoot;
	
	private Gson gson = new Gson();	 
	
	public static class PluginInfos {
		List<PluginInfoVersion> plugins = new ArrayList<>();
	}
	
	public static class ItemInfo {
		String type;
		String pluginId;
		int version;
		long publishedDate;
		Map<String, String> icon;
		Map<String, String> image;
		Map<String, String> name;
		Map<String, String> description;
	}
	
	public static class ItemsJson {
		public List<ItemInfo> items = new ArrayList<>();
	}
	
	
	public static class PluginInfoVersion {
		public boolean active;
		public String pluginId;
		public int version;
		public String publishedDate;
		
		public String name;
		public String description;
		
		public String osfUrl;
		public String imagePath;
		public String imageUrl;
		public String iconPath;
		public String iconUrl;

		public String iconLocalPath() {
			if (iconPath == null || !iconPath.startsWith("@")) {
				return "";
			}
			return "res/" + iconPath.substring(1);
		}

		public String imageLocalPath() {
			if (imagePath == null || !imagePath.startsWith("@")) {
				return "";
			}
			return "res/" + imagePath.substring(1);
		}
		
		public String getImageUrl() {
			return !imagePath.startsWith("@") ? "" : "/" + getPath() + "/res/" + imagePath.substring(1);
		}

		public String getIconUrl() {
			return !iconPath.startsWith("@") ? "" : "/" + getPath() + "/res/" + iconPath.substring(1);
		}

		public boolean isNightly() {
			return publishedDate == null || publishedDate.equals(NIGHTLY);
		}
		
		private String getOsfName() {
			return this.pluginId + "-" + this.version + ".osf";
		}

		private String getPath() {
			return UPLOADS_PLUGINS + "/" + this.pluginId + "/" + this.version;
		}

	}
	
	public PluginInfos getPluginsInfo(String os, String version, boolean nighlty) throws IOException {
		PluginInfos pi = getPluginsInfo();
		PluginInfos res = new PluginInfos();
		for(PluginInfoVersion p : pi.plugins) {
			if (p.isNightly() && p.active) {
				res.plugins.add(p);
			}
		}
		return res;
	}
 	
	public List<PluginInfoVersion> getPluginsAdminInfo() throws IOException {
		PluginInfos infos = getPluginsInfo();
		return infos.plugins;
	}

	private PluginInfos getPluginsInfo() throws IOException {
		PluginInfos infos = new PluginInfos();
		File file = new File(pathToRoot, UPLOADS_PLUGINS);
		for (File pluginFolder : file.listFiles()) {
			if (pluginFolder.isDirectory()) {
				parsePlugin(pluginFolder, infos.plugins);
			}
		}
		return infos;
	}

	private void parsePlugin(File pluginFolder, List<PluginInfoVersion> plugins) throws IOException {
		File pluginInfo = new File(pluginFolder, INFO_JSON);
		if (pluginInfo.exists()) {
			FileReader reader = new FileReader(pluginInfo);
			PluginInfos infos = gson.fromJson(reader, PluginInfos.class);
			plugins.addAll(infos.plugins);
			reader.close();
			return;
		}
		List<PluginInfoVersion> versions = new ArrayList<>();
		for (File versionFolder : pluginFolder.listFiles()) {
			File itemsJson = new File(versionFolder, ITEMS_JSON);
			if (itemsJson.exists()) {
				FileReader reader = new FileReader(itemsJson);
				PluginInfoVersion infoVersion = parseItemsJson(reader);
				reader.close();
				String reason = null;
				if (infoVersion.version == 0) {
					reason = "version is 0";
				} else if (!Algorithms.objectEquals(pluginFolder.getName(), infoVersion.pluginId)) {
					reason = String.format("plugin ids mismiatched %s != %s", pluginFolder.getName(),
							infoVersion.pluginId);
				} else if (!Algorithms.objectEquals(versionFolder.getName(), infoVersion.version + "")) {
					reason = String.format("plugin versions mismiatched %s != %s", pluginFolder.getName(),
							infoVersion.pluginId);
				}
				if (reason != null) {
					System.err.printf("Error processing plugin (skip) %s/%s: %s\n", pluginFolder.getName(),
							versionFolder.getName(), reason);
				} else {
					versions.add(infoVersion);
				}
			}
		}
		Collections.sort(versions, new Comparator<PluginInfoVersion>() {

			@Override
			public int compare(PluginInfoVersion o1, PluginInfoVersion o2) {
				return -Integer.compare(o1.version, o2.version);
			}
		});
		for (int i = 0; i < versions.size(); i++) {
			PluginInfoVersion v = versions.get(i);
			if (i == 0) {
				v.active = true;
				if (v.publishedDate == null) {
					v.publishedDate = NIGHTLY;
				} else {
					break;
				}
			} else if (v.publishedDate != null) {
				v.active = true;
				break;
			}
		}
		plugins.addAll(versions);
	}

	private PluginInfoVersion parseItemsJson(Reader reader) throws IOException {
		ItemsJson infos = gson.fromJson(reader, ItemsJson.class);
		PluginInfoVersion infoVersion = new PluginInfoVersion();
		for (ItemInfo item : infos.items) {
			if (item.type.equals("PLUGIN")) {
				infoVersion.pluginId = item.pluginId;
				infoVersion.version = item.version;
				infoVersion.name = item.name.get("");
				infoVersion.publishedDate = item.publishedDate == 0 ? null : new Date(item.publishedDate).toString();
				infoVersion.description = item.description.get("");
				infoVersion.imagePath = item.image.get("");
				infoVersion.iconPath = item.icon.get("");
				infoVersion.iconUrl = infoVersion.getIconUrl();
				infoVersion.imageUrl = infoVersion.getImageUrl();
				infoVersion.osfUrl =  "/" + infoVersion.getPath() + "/" + infoVersion.getOsfName();
			}
		}
		return infoVersion;
	}

	public Map<String, ?> uploadFile(@Valid @NotNull @NotEmpty MultipartFile file) throws IOException {
		File fl = new File(pathToRoot, UPLOADS_PLUGINS + "/plugin.osf");
		InputStream is = file.getInputStream();
		FileOutputStream fous = new FileOutputStream(fl);
		Algorithms.streamCopy(is, fous);
		is.close();
		fous.close();
		ZipInputStream zis = null;
		try {
			zis = new ZipInputStream(new FileInputStream(fl));
			ZipEntry entry = null;
			PluginInfoVersion version = null;
			while ((entry = zis.getNextEntry()) != null) {
				if (entry.getName().equals(ITEMS_JSON)) {
					version = parseItemsJson(new InputStreamReader(zis));
					break;
				}
			}
			zis.close();
			if (version == null) {
				return Map.of("status", "error", "message", "Plugins doesn't have items json");

			}
			PluginInfos infos = getPluginsInfo();
			String errorMsg = null;
			for (PluginInfoVersion v : infos.plugins) {
				if (v.pluginId.equals(version.pluginId)
						&& (v.version > version.version || (v.version == version.version && !v.isNightly()))) {
					errorMsg= String.format("Newer version of plugin %s %d already published %d", v.pluginId, v.version,
							version.version);
					break;
				}
			}
			if (errorMsg != null) {
				return Map.of("status", "error", "message", errorMsg);
			}
			File parentFld = new File(pathToRoot, version.getPath());
			parentFld.mkdirs();
			File targetFile = new File(parentFld, version.getOsfName());
			if(targetFile.exists()) {
				targetFile.delete();
			}
			fl.renameTo(targetFile);
			zis = new ZipInputStream(new FileInputStream(targetFile));
			List<String> files = new ArrayList<>();
			while ((entry = zis.getNextEntry()) != null) {
				File ofl = new File(entry.getName());
				if (!ofl.getName().startsWith(".")) {
					files.add(entry.getName());
				}
				if (entry.getName().equals(ITEMS_JSON) || entry.getName().equals(version.iconLocalPath())
						|| entry.getName().equals(version.imageLocalPath())) {
					File items = new File(parentFld, entry.getName());
					items.getParentFile().mkdirs();
					fous = new FileOutputStream(items);
					Algorithms.streamCopy(zis, fous);
					fous.close();
				}
			}
			return Map.of("status", "ok", "pluginid", version.pluginId, "version", version.version, "name",
					version.name, "files", files);
		} finally {
			if (zis != null) {
				zis.close();
			}
		}
		
		
		
	}


}
