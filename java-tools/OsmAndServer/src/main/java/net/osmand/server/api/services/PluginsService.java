package net.osmand.server.api.services;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import net.osmand.util.Algorithms;

@Service
public class PluginsService {
	
	private static final String INFO_JSON = "info.json";
	private static final String ITEMS_JSON = "items.json";
	
	@Value("${osmand.files.location}")
    private String pathToPluginFiles;
	
	private Gson gson = new Gson();	 
	
	public static class PluginInfos {
		List<PluginInfoVersion> plugins = new ArrayList<>();
	}
	
	public static class ItemInfo {
		String type;
		String pluginId;
		int version;
		boolean nightly;
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
		public boolean nightly;
		public String imageUrl;
		public String iconUrl;
		public String name;
		public String description;
	}
	
	public PluginInfos getPluginsInfo(String os, String version, boolean nighlty) throws IOException {
		PluginInfos pi = getPluginsInfo();
		PluginInfos res = new PluginInfos();
		for(PluginInfoVersion p : pi.plugins) {
			if(nighlty == p.nightly && p.active) {
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
		// TODO cache & recalculate
		PluginInfos infos = new PluginInfos();
		File file = new File(pathToPluginFiles, "uploads/plugins");
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
				ItemsJson infos = gson.fromJson(reader, ItemsJson.class);
				PluginInfoVersion infoVersion = new PluginInfoVersion();
				for (ItemInfo item : infos.items) {
					if (item.type.equals("PLUGIN")) {
						infoVersion.version = item.version;
						infoVersion.name = item.name.get("");
						infoVersion.nightly = item.nightly;
						infoVersion.description = item.description.get("");
						infoVersion.imageUrl = item.image.get("");
						infoVersion.iconUrl = item.icon.get("");
						infoVersion.pluginId = item.pluginId;
					}
				}
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
		for (PluginInfoVersion i : versions) {
			if (!i.nightly) {
				i.active = true;
				break;
			}
		}
		for (PluginInfoVersion i : versions) {
			if (i.nightly) {
				i.active = true;
				break;
			}
		}
		plugins.addAll(versions);
	}


}
