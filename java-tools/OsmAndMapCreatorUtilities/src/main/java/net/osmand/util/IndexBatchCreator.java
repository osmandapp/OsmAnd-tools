package net.osmand.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.SimpleFormatter;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xmlpull.v1.XmlPullParserException;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DockerClientBuilder;
import com.sun.management.OperatingSystemMXBean;

import net.osmand.IndexConstants;
import net.osmand.MapCreatorVersion;
import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.util.CountryOcbfGeneration.CountryRegion;
import rtree.RTree;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.batch.BatchClient;
import software.amazon.awssdk.services.batch.model.DescribeJobsRequest;
import software.amazon.awssdk.services.batch.model.DescribeJobsResponse;
import software.amazon.awssdk.services.batch.model.JobDetail;
import software.amazon.awssdk.services.batch.model.JobStatus;
import software.amazon.awssdk.services.batch.model.SubmitJobRequest;
import software.amazon.awssdk.services.batch.model.SubmitJobResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;


public class IndexBatchCreator {

	private static final int INMEM_LIMIT = 2000;
	private static final long TIMEOUT_TO_CHECK_AWS = 15000;
	private static final long TIMEOUT_TO_CHECK_DOCKER = 15000;

	protected static final Log log = PlatformUtil.getLog(IndexBatchCreator.class);

	public static final String GEN_LOG_EXT = ".gen.log";



	public static class RegionCountries {
		String namePrefix = ""; // for states of the country
		String nameSuffix = "";
		Map<String, RegionSpecificData> regionNames = new LinkedHashMap<String, RegionSpecificData>();
		String siteToDownload = "";
	}
	
	private static class ExternalJobDefinition {
		Map<String, String> params = new LinkedHashMap<>();
		String name;
		String type;
		// On AWS name of the queue
		String queue;
		// Docker
		int slotsPerJob = 1;
		int freeRamToStartPerc = 0;
		int freeRamToStopPerc = 0;
		int order = 0;
		// AWS
		String definition;
		
		// filter
		int sizeUpToMB = -1;
		Set<String> excludedRegions = new TreeSet<>();
		List<String> excludePatterns = new ArrayList<>();
		
		
		List<DockerPendingGeneration> pendingGenerations = new ArrayList<>();

	}

	private static class RegionSpecificData {
		public String downloadName;
		public boolean indexSRTM = true;
		public boolean indexPOI = true;
		public boolean indexTransport = true;
		public boolean indexAddress = true;
		public boolean indexMap = true;
		public boolean indexRouting = true;
        public boolean indexByProximity = true;
	}
	
	private static class AwsPendingGeneration {
		SubmitJobResponse response;
		String s3Url;
		String targetFileName;
	}
	
	private static class DockerPendingGeneration {
		public ExternalJobDefinition jd;
		public List<String> cmd = new ArrayList<String>();
		public String image;
		public String name;
		public List<Bind> binds = new ArrayList<>();
		public List<String> envs = new ArrayList<String>();
		public CreateContainerResponse container;
	}
	
	private static class LocalPendingGeneration {
		public String mapFileName;
		public File file;
		public String regionName;
		public RegionSpecificData rdata;
	}


	// process atributtes
	File skipExistingIndexes;
	MapZooms mapZooms = null;
	Integer zoomWaySmoothness = null;

	File osmDirFiles;
	File indexDirFiles;
	File workDir;
	String srtmDir;
	

	List<LocalPendingGeneration> localPendingGenerations = new ArrayList<>();
	List<ExternalJobDefinition> externalJobQueues = new ArrayList<>();
	
	
	//// AWS gen block
	List<AwsPendingGeneration> awsPendingGenerations = new ArrayList<>();
	List<AwsPendingGeneration> awsFailedGenerations = new ArrayList<>();
	int awsStatushash = 0;
	int awsSucceeded = 0;
	int awsFailed = 0;
	
	/// DOCKER gen block
	DockerClient dockerClient;
	int dockerSlots = 4;
	List<DockerPendingGeneration> dockerRunningGenerations = new ArrayList<>();
	List<DockerPendingGeneration> dockerRescheduledGenerations = new ArrayList<>();
	List<DockerPendingGeneration> dockerFailedGenerations = new ArrayList<>();
	
	boolean indexPOI = false;
	boolean indexTransport = false;
	boolean indexAddress = false;
	boolean indexMap = false;
	boolean indexRouting = false;
	boolean indexByProximity = true;
	private String wget;

	private DBDialect osmDbDialect;
	private String renderingTypesFile;
	private SimpleDateFormat sdf;


	public static void main(String[] args) throws Exception {
		IndexBatchCreator creator = new IndexBatchCreator();
		if (args == null || args.length == 0) {
			System.out
					.println("Please specify -local parameter or path to batch.xml configuration file as 1 argument.");
			throw new IllegalArgumentException(
					"Please specify -local parameter or path to batch.xml configuration file as 1 argument.");
		}
		String name = args[0];
		InputStream stream;
		try {
			stream = new FileInputStream(name);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException("XML configuration file not found : " + name, e);
		}
		List<RegionCountries> countriesToProcess  = null;
		try {
			Document params = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(stream);
			countriesToProcess = creator.setupProcess(params);
		} catch (Exception e) {
			System.out.println("XML configuration file could not be read from " + name);
			log.error("XML configuration file could not be read from " + name + ": " + e.getMessage(), e);
			throw e;
		} finally {
			safeClose(stream, "Error closing stream for " + name);
		}
		creator.runBatch(countriesToProcess);
	}

	public List<RegionCountries> setupProcess(Document doc)
			throws SAXException, IOException, ParserConfigurationException, XmlPullParserException {
		NodeList list = doc.getElementsByTagName("process");
		if (list.getLength() != 1) {
			throw new IllegalArgumentException("You should specify exactly 1 process element!");
		}
		Element process = (Element) list.item(0);
		IndexCreator.REMOVE_POI_DB = true;
		String file = process.getAttribute("skipExistingIndexesAt");
		if (file != null && new File(file).exists()) {
			skipExistingIndexes = new File(file);
		}
		wget = process.getAttribute("wget");

		indexPOI = Boolean.parseBoolean(process.getAttribute("indexPOI"));
		indexMap = Boolean.parseBoolean(process.getAttribute("indexMap"));
		indexRouting = process.getAttribute("indexRouting") == null
				|| process.getAttribute("indexRouting").equalsIgnoreCase("true");
		indexTransport = Boolean.parseBoolean(process.getAttribute("indexTransport"));
		indexAddress = Boolean.parseBoolean(process.getAttribute("indexAddress"));
		indexByProximity = Boolean.parseBoolean(process.getAttribute("indexByProximity"));
		parseProcessAttributes(process);

		list = doc.getElementsByTagName("process_attributes");
		if (list.getLength() == 1) {
			parseProcessAttributes((Element) list.item(0));
		}

		String dir = process.getAttribute("directory_for_osm_files");
		if (dir == null || !new File(dir).exists()) {
			throw new IllegalArgumentException(
					"Please specify directory with .osm or .osm.bz2 files as directory_for_osm_files (attribute)" //$NON-NLS-1$
							+ dir);
		}
		osmDirFiles = new File(dir);

		srtmDir = process.getAttribute("directory_for_srtm_files");

		dir = process.getAttribute("directory_for_index_files");
		if (dir == null || !new File(dir).exists()) {
			throw new IllegalArgumentException(
					"Please specify directory with generated index files  as directory_for_index_files (attribute)"); //$NON-NLS-1$
		}
		indexDirFiles = new File(dir);
		workDir = indexDirFiles;
		dir = process.getAttribute("directory_for_generation");
		if (dir != null && new File(dir).exists()) {
			workDir = new File(dir);
		}

		parseJobDefinitions(process.getElementsByTagName("external"), externalJobQueues);
		List<RegionCountries> countriesToDownload = new ArrayList<RegionCountries>();
		parseCountriesToDownload(doc, countriesToDownload);
		return countriesToDownload;
	}

	private void parseJobDefinitions(NodeList nodeList, List<ExternalJobDefinition> jobQueues) {
		for (int j = 0; j < nodeList.getLength(); j++) {
			Element external = (Element) nodeList.item(j);
			if (!Algorithms.isEmpty(external.getAttribute("dockerSlots"))) {
				dockerSlots = Integer.parseInt(external.getAttribute("dockerSlots"));
			}
			NodeList jobs = external.getElementsByTagName("job");
			for (int k = 0; k < jobs.getLength(); k++) {
				Element jbe = (Element) jobs.item(k);
				ExternalJobDefinition jd = new ExternalJobDefinition();
				jd.definition = jbe.getAttribute("definition");
				if (!Algorithms.isEmpty(jbe.getAttribute("slotsPerJob"))) {
					jd.slotsPerJob = Integer.parseInt(jbe.getAttribute("slotsPerJob"));
				}
				if (!Algorithms.isEmpty(jbe.getAttribute("order"))) {
					jd.order = Integer.parseInt(jbe.getAttribute("order"));
				}
				if (!Algorithms.isEmpty(jbe.getAttribute("freeRamToStartPerc"))) {
					jd.freeRamToStartPerc = Integer.parseInt(jbe.getAttribute("freeRamToStartPerc"));
				}
				if (!Algorithms.isEmpty(jbe.getAttribute("freeRamToStopPerc"))) {
					jd.freeRamToStopPerc = Integer.parseInt(jbe.getAttribute("freeRamToStopPerc"));
				}
				jd.name = jbe.getAttribute("name");
				jd.type = jbe.getAttribute("type");
				jd.queue = jbe.getAttribute("queue");
				if (!Algorithms.isEmpty(jbe.getAttribute("sizeUpToMB"))) {
					jd.sizeUpToMB = Integer.parseInt(jbe.getAttribute("sizeUpToMB"));
				}
				NodeList params = jbe.getElementsByTagName("parameter");
				for (int l = 0; l < params.getLength(); l++) {
					Element jbep = (Element) params.item(l);
					jd.params.put(jbep.getAttribute("k"), jbep.getAttribute("v"));
				}
				NodeList filters = jbe.getElementsByTagName("filter");
				for (int l = 0; l < filters.getLength(); l++) {
					Element f = (Element) filters.item(l);
					if (!Algorithms.isEmpty(f.getAttribute("exclude"))) {
						jd.excludedRegions.add(f.getAttribute("exclude").toLowerCase());
					}
					if (!Algorithms.isEmpty(f.getAttribute("excludePattern"))) {
						jd.excludePatterns.add(f.getAttribute("excludePattern").toLowerCase());
					}
				}
				jd.name = jbe.getAttribute("name");
				jobQueues.add(jd);
			}
		}
	}

	private void parseCountriesToDownload(Document doc, List<RegionCountries> countriesToDownload) throws IOException, XmlPullParserException {
		NodeList regions = doc.getElementsByTagName("regions");
		for (int i = 0; i < regions.getLength(); i++) {
			Element el = (Element) regions.item(i);
			if (!Boolean.parseBoolean(el.getAttribute("skip"))) {
				RegionCountries countries = new RegionCountries();
				countries.siteToDownload = el.getAttribute("siteToDownload");
				if (countries.siteToDownload == null) {
					continue;
				}
				countries.namePrefix = el.getAttribute("region_prefix");
				if (countries.namePrefix == null) {
					countries.namePrefix = "";
				}
				countries.nameSuffix = el.getAttribute("region_suffix");
				if (countries.nameSuffix == null) {
					countries.nameSuffix = "";
				}
				
				NodeList nRegionsList = el.getElementsByTagName("regionList");
				for (int j = 0; j < nRegionsList.getLength(); j++) {
					Element nregionList = (Element) nRegionsList.item(j);
					String url = nregionList.getAttribute("url");
					if (url != null) {
						String filterStartWith = nregionList.getAttribute("filterStartsWith");
						String filterContains = nregionList.getAttribute("filterContains");
						CountryOcbfGeneration ocbfGeneration = new CountryOcbfGeneration();
						log.warn("Download region list from " + url);
						CountryRegion regionStructure = ocbfGeneration.parseRegionStructure(new URL(url).openStream());
						Iterator<CountryRegion> it = regionStructure.iterator();
						int total = 0;
						int before = countries.regionNames.size();
						while (it.hasNext()) {
							CountryRegion cr = it.next();
							if (cr.getDownloadName().contains("_basemap")) {
								// skip basemap
								continue;
							}

							if (cr.map && !cr.jointMap) {
								total++;
								if (!Algorithms.isEmpty(filterStartWith) && !cr.getDownloadName().toLowerCase()
										.startsWith(filterStartWith.toLowerCase())) {
									continue;
								}
								if (!Algorithms.isEmpty(filterContains)
										&& !cr.getDownloadName().toLowerCase().contains(filterContains.toLowerCase())) {
									continue;
								}
								RegionSpecificData dt = new RegionSpecificData();
								dt.downloadName = cr.getDownloadName();
								countries.regionNames.put(dt.downloadName, dt);
							}
						}
						log.warn(String.format("Accepted %d from %d", countries.regionNames.size() - before, total));
					}
				}
				NodeList ncountries = el.getElementsByTagName("region");
				log.info("Region to download " + countries.siteToDownload);
				for (int j = 0; j < ncountries.getLength(); j++) {
					Element ncountry = (Element) ncountries.item(j);
					String name = ncountry.getAttribute("name");
					RegionSpecificData data = new RegionSpecificData();
					data.indexSRTM = ncountry.getAttribute("indexSRTM") == null
							|| ncountry.getAttribute("indexSRTM").equalsIgnoreCase("true");
					String index = ncountry.getAttribute("index");
					if (index != null && index.length() > 0) {
						data.indexAddress = index.contains("address");
						data.indexMap = index.contains("map");
						data.indexTransport = index.contains("transport");
						data.indexRouting = index.contains("routing");
						data.indexPOI = index.contains("poi");
					}
					String dname = ncountry.getAttribute("downloadName");
					data.downloadName = dname == null || dname.length() == 0 ? name : dname;
					if (name != null && !Boolean.parseBoolean(ncountry.getAttribute("skip"))) {
						countries.regionNames.put(name, data);
					}
				}
				countriesToDownload.add(countries);

			}
		}
	}

	private void parseProcessAttributes(Element process) {
		String zooms = process.getAttribute("mapZooms");
		if(zooms == null || zooms.length() == 0){
			mapZooms = MapZooms.getDefault();
		} else {
			mapZooms = MapZooms.parseZooms(zooms);
		}

		String szoomWaySmoothness = process.getAttribute("zoomWaySmoothness");
		if(szoomWaySmoothness != null && !szoomWaySmoothness.isEmpty()){
			zoomWaySmoothness = Integer.parseInt(szoomWaySmoothness);
		}
		renderingTypesFile = process.getAttribute("renderingTypesFile");

		String osmDbDialect = process.getAttribute("osmDbDialect");
		if(osmDbDialect != null && osmDbDialect.length() > 0){
			try {
				this.osmDbDialect = DBDialect.valueOf(osmDbDialect.toUpperCase());
			} catch (RuntimeException e) {
			}
		}
	}

	public void runBatch(List<RegionCountries> countriesToDownload) {
		Set<String> alreadyGeneratedFiles = new LinkedHashSet<String>();
		if (!countriesToDownload.isEmpty()) {
			downloadFilesAndGenerateIndex(countriesToDownload, alreadyGeneratedFiles);
		}
		generateLocalFolderIndexes(alreadyGeneratedFiles);
		// run check in parallel
		new Thread(new Runnable() {
			@Override
			public void run() {
				waitAwsJobsToFinish(TIMEOUT_TO_CHECK_AWS * 4);
			}
		}).start();
		// run check in parallel
		new Thread(new Runnable() {
			@Override
			public void run() {
				waitDockerJobsToFinish(TIMEOUT_TO_CHECK_DOCKER * 2);
			}
		}).start();
		log.info("Generate local " + localPendingGenerations.size() + " maps");
		for (LocalPendingGeneration lp : localPendingGenerations) {
			generateLocalIndex(lp.file, lp.regionName, lp.mapFileName, lp.rdata, alreadyGeneratedFiles);
		}
		waitAwsJobsToFinish(TIMEOUT_TO_CHECK_AWS);
		waitDockerJobsToFinish(TIMEOUT_TO_CHECK_DOCKER);
		log.info("GENERATING INDEXES FINISHED ");
		if (awsFailedGenerations.size() > 0) {
			throw new IllegalStateException("There are " + awsFailedGenerations.size() + " aws failed generations");
		}
		if (dockerFailedGenerations.size() > 0) {
			throw new IllegalStateException("There are " + dockerFailedGenerations.size() + " docker  failed generations");
		}
	}


	private void waitDockerJobsToFinish(long timeout) {
		while (true) {
			int pending = 0;
			for (ExternalJobDefinition jd : externalJobQueues) {
				pending += jd.pendingGenerations.size();
			}
			int rescheduled = dockerRescheduledGenerations.size();
			int running = dockerRunningGenerations.size();
			int total = pending + rescheduled + running;

			if (total == 0) {
				return;
			}

			List<String> names = new ArrayList<String>();
			for (DockerPendingGeneration d : dockerRunningGenerations) {
				names.add(d.name);
			}

			long freeRamPerc = getFreeRamPercentage();
			log.warn(String.format("Waiting %d docker jobs to complete (Ram free %d). Pending: %d, Rescheduled: %d, Running: %d: %s", freeRamPerc,
					total, pending, rescheduled, running, names));

			waitDockerJobsIteration(freeRamPerc);
			try {
				Thread.sleep(timeout);
			} catch (InterruptedException e) {
			}
		}
	}

	private long getFreeRamPercentage() {
		// Try Linux-specific method first by reading /proc/meminfo
		File meminfo = new File("/proc/meminfo");
		if (meminfo.exists()) {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(meminfo)))) {
				String line;
				long totalMem = -1;
				long availableMem = -1;
				while ((line = reader.readLine()) != null) {
					if (line.startsWith("MemTotal:")) {
						totalMem = Long.parseLong(line.split("\\s+")[1]);
					} else if (line.startsWith("MemAvailable:")) {
						availableMem = Long.parseLong(line.split("\\s+")[1]);
					}
				}
				if (totalMem > 0 && availableMem > 0) {
					return (availableMem * 100) / totalMem;
				}
			} catch (IOException | NumberFormatException e) {
				log.warn("Could not read or parse /proc/meminfo, falling back to MXBean.", e);
			}
		}

		// Fallback to MXBean for non-Linux or if /proc/meminfo fails
		try {
			OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
			long freeMemory = osBean.getFreeMemorySize();
			long totalMemory = osBean.getTotalMemorySize();
			if (totalMemory > 0) {
				return (freeMemory * 100) / totalMemory;
			}
		} catch (Exception e) {
			log.warn("Could not determine free RAM percentage using OperatingSystemMXBean.", e);
		}

		log.warn("Could not determine free RAM percentage. Assuming 100%.");
		return 100; // Assume enough RAM if all checks fail
	}
	
	protected List<DockerPendingGeneration> createQueueEquallyDistributed(List<List<DockerPendingGeneration>> all) {
		List<DockerPendingGeneration> queue = new ArrayList<IndexBatchCreator.DockerPendingGeneration>();
		boolean added = true;
		int ind = 0;
		while (added) {
			added = false;
			for (int i = 0; i < all.size(); i++) {
				List<DockerPendingGeneration> lst = all.get(i);
				if (ind < lst.size()) {
					queue.add(lst.get(ind));
					added = true;
				}
			}
			ind++;
		}
		return queue;
	}

	private void checkStatusRunningContainers() {
		Iterator<DockerPendingGeneration> runningIt = dockerRunningGenerations.iterator();
		while (runningIt.hasNext()) {
			DockerPendingGeneration p = runningIt.next();
			try {
				InspectContainerResponse res = dockerClient.inspectContainerCmd(p.container.getId()).exec();
				if (!res.getState().getRunning()) {
					Long l = res.getState().getExitCodeLong();
					if (l == null || l.longValue() != 0) {
						log.info(String.format("FAILED GENERATION %s - container %s", p.name, p.container.getId()));
						if (!dockerFailedGenerations.contains(p)) {
							dockerRescheduledGenerations.add(p);
							dockerFailedGenerations.add(p);
							dockerClient.removeContainerCmd(p.container.getId()).exec();
						}
					} else {
						log.info(String.format("Finished %s container %s at %s (started %s).",
								getDuration(res.getState()), res.getName(), res.getState().getFinishedAt(),
								res.getState().getStartedAt()));
						// remove if it's failed before
						dockerFailedGenerations.remove(p);
						dockerClient.removeContainerCmd(p.container.getId()).exec();
					}
					runningIt.remove();
				}
			} catch (RuntimeException e) {
				log.error("Error inspecting container " + p.name + ": " + e.getMessage(), e);
				runningIt.remove(); // Assume it's gone
			}
		}
	}
	
	private boolean stopIfNotEnoughRam(long freeRamPerc) {
		DockerPendingGeneration lastStarted = null;
		if (!dockerRunningGenerations.isEmpty()) {
			// The last one added to the list is the most recently started
			lastStarted = dockerRunningGenerations.get(dockerRunningGenerations.size() - 1);
		}
		if (lastStarted != null && lastStarted.jd.freeRamToStopPerc > 0
				&& freeRamPerc < lastStarted.jd.freeRamToStopPerc) {
			log.warn(String.format("Low RAM detected (%d%% free). Stopping last started container %s to reschedule.",
					freeRamPerc, lastStarted.name));
			try {
				dockerClient.stopContainerCmd(lastStarted.container.getId()).exec();
				dockerClient.removeContainerCmd(lastStarted.container.getId()).exec();
			} catch (Exception e) {
				log.error("Failed to stop container for rescheduling: " + lastStarted.name, e);
			}
			lastStarted.container = null; // Mark as pending
			dockerRunningGenerations.remove(lastStarted);
			dockerRescheduledGenerations.add(lastStarted);
			return true; // Only stop one container per check
		}
		return false;
	}
	
	private List<DockerPendingGeneration> getQueueToRun() {
		// Build a list of jobs to potentially start, giving priority to rescheduled
		List<DockerPendingGeneration> queueToRun = new ArrayList<>(dockerRescheduledGenerations);
		// Find the first queue with pending jobs and add them
		Collections.sort(externalJobQueues, new Comparator<ExternalJobDefinition>() {

			@Override
			public int compare(ExternalJobDefinition o1, ExternalJobDefinition o2) {
				return Integer.compare(o1.order, o2.order);
			}
		});
		for (ExternalJobDefinition jd : externalJobQueues) {
			queueToRun.addAll(jd.pendingGenerations);
		}
		return queueToRun;
	}
	
	@SuppressWarnings("deprecation")
	private void startContainers(List<DockerPendingGeneration> queue, long freeRamPerc) {
		Iterator<DockerPendingGeneration> startIt = queue.iterator();
		int slotsLeft = dockerSlots;
		for (DockerPendingGeneration running : dockerRunningGenerations) {
			slotsLeft -= running.jd.slotsPerJob;
		}
		while (startIt.hasNext() && slotsLeft > 0) {
			DockerPendingGeneration p = startIt.next();
			if (slotsLeft >= p.jd.slotsPerJob) {
				// Check if we have enough RAM to start
				if (p.jd.freeRamToStartPerc > 0 && freeRamPerc < p.jd.freeRamToStartPerc) {
					log.warn(String.format("Delaying start of %s due to low RAM. Free: %d%%, Required: >%d%%", p.name,
							freeRamPerc, p.jd.freeRamToStartPerc));
					return;
				}
				try {
					// In future: we can find & remove container if it exists with same name
					p.jd.pendingGenerations.remove(p);
					dockerRescheduledGenerations.remove(p);
					p.container = dockerClient.createContainerCmd(p.image).withBinds(p.binds).withCmd(p.cmd)
							.withEnv(p.envs).withName(p.name).exec();
					dockerClient.startContainerCmd(p.container.getId()).exec();
					log.info("Started container " + p.name);
					dockerRunningGenerations.add(p);
					slotsLeft -= p.jd.slotsPerJob;
				} catch (Exception e) {
					log.error("Failed to start container " + p.name, e);
					if (!dockerFailedGenerations.contains(p)) {
						dockerFailedGenerations.add(p);
						dockerRescheduledGenerations.add(p);
					}
					return;
				}
			}
		}
	}

	private synchronized void waitDockerJobsIteration(long freeRamPerc) {
		if (dockerClient == null) {
			dockerClient = DockerClientBuilder.getInstance().build();
		}
		// 1. Check status of running containers
		checkStatusRunningContainers();
		// 2. Check for low RAM and stop the last started container if necessary
		if(stopIfNotEnoughRam(freeRamPerc)) {
			// Only stop one container per check
			return;
		}
		// 3. Start new containers from queues sequentially
		startContainers(getQueueToRun(), freeRamPerc);
	}

	private String getDuration(ContainerState state) {
		if (sdf == null) {
			sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		}
		try {
			long finish = sdf.parse(state.getFinishedAt()).getTime();
			long start = sdf.parse(state.getStartedAt()).getTime();
			long s = (finish - start) / 1000;
			return String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60));
		} catch (Exception e) {
			log.warn(e.getMessage(), e);
			return "";
		}
	}

	private void waitAwsJobsToFinish(long timeout) {
		log.info(String.format("Waiting %d aws jobs to complete...", awsPendingGenerations.size()));
		while (awsPendingGenerations.size() > 0) {
			waitAwsJobsIteration();
			try {
				Thread.sleep(timeout);
			} catch (InterruptedException e) {
			}
		}
	}

	private synchronized void waitAwsJobsIteration() {
		Map<String, JobDetail> awsStatus = new LinkedHashMap<>();
		List<String> jobIds = new ArrayList<String>();
		for (AwsPendingGeneration p : awsPendingGenerations) {
			jobIds.add(p.response.jobId());
			if (jobIds.size() > 50) {
				getJobStatus(jobIds, awsStatus);
				jobIds.clear();
			}
		}
		getJobStatus(jobIds, awsStatus);
		Iterator<AwsPendingGeneration> it = awsPendingGenerations.iterator();
		int readytorun = 0;
		int running = 0;
		int starting = 0;
		while (it.hasNext()) {
			AwsPendingGeneration gen = it.next();
			JobDetail status = awsStatus.get(gen.response.jobId());
			if (status == null) {
				continue;
			}
			JobStatus js = JobStatus.fromValue(status.statusAsString());
			if (js == JobStatus.RUNNABLE) {
				readytorun++;
			} else if (js == JobStatus.RUNNING) {
				running++;
			} else if (js == JobStatus.STARTING) {
				starting++;
			} else if (js == JobStatus.SUCCEEDED) {
				awsSucceeded++;
				try {
					S3Client client = S3Client.builder().build();
					String s3url = gen.s3Url;
					if (s3url.startsWith("s3://")) {
						s3url = s3url.substring("s3://".length());
					}
					int i = s3url.indexOf('/');
					String bucket = s3url.substring(0, i);
					String key = s3url.substring(i + 1);
					GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
					ResponseInputStream<GetObjectResponse> obj = client.getObject(request);
					FileOutputStream fous = new FileOutputStream(new File(indexDirFiles, gen.targetFileName));
					Algorithms.streamCopy(obj, fous);
					obj.close();
					fous.close();
					it.remove();
				} catch (Exception e) {
					log.error(String.format("Error retrieving result from S3 %s: %s", gen.s3Url, e.getMessage()), e);
				}
			} else if (js == JobStatus.FAILED) {
				// gen.failedDetail = status;
				awsFailed++;
				it.remove();
				awsFailedGenerations.add(gen);
				log.error(String.format("! Failed generation %s, job id %s: %s", gen.targetFileName, status.jobId(),
						status.statusReason()));
			}
		}
		int hash = awsPendingGenerations.size() + readytorun * 7 + starting * 11 + running * 17 + awsSucceeded * 41 + awsFailed * 107;
		if (awsStatushash != hash) {
			log.info(String.format("Pending %d aws jobs: ready to run %d, starting %d running %d - succeeded %d, failed %d ...",
					awsPendingGenerations.size(), readytorun, starting, running, awsSucceeded, awsFailed));
			awsStatushash = hash;
		}
	}

	private void getJobStatus(List<String> jobIds, Map<String, JobDetail> awsStatus) {
		try {
			BatchClient client = BatchClient.builder().build();
			DescribeJobsResponse response = client.describeJobs(DescribeJobsRequest.builder().jobs(jobIds).build());
			for (JobDetail jd : response.jobs()) {
				awsStatus.put(jd.jobId(), jd);
			}
		} catch (Exception e) {
			log.error("Error retrieving status for batch jobs:" + e.getMessage(), e);
		}

	}

	protected void downloadFilesAndGenerateIndex(List<RegionCountries> countriesToDownload,
			Set<String> alreadyGeneratedFiles) {
		// clean before downloading
		// for(File f : osmDirFiles.listFiles()){
		// log.info("Delete old file " + f.getName()); //$NON-NLS-1$
		// f.delete();
		// }

		for (RegionCountries regionCountries : countriesToDownload) {
			String prefix = regionCountries.namePrefix;
			String site = regionCountries.siteToDownload;
			String suffix = regionCountries.nameSuffix;
			for (String name : regionCountries.regionNames.keySet()) {
				RegionSpecificData regionSpecificData = regionCountries.regionNames.get(name);
				name = name.toLowerCase();
				String url = MessageFormat.format(site, regionSpecificData.downloadName);

				String regionName = prefix + name;
				String fileName = Algorithms.capitalizeFirstLetterAndLowercase(prefix + name + suffix);
//				log.warn("----------- Check existing " + fileName);
				if (skipExistingIndexes != null) {
					File bmif = new File(skipExistingIndexes,
							fileName + "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_MAP_INDEX_EXT);
					File bmifz = new File(skipExistingIndexes, bmif.getName() + ".zip");
					if (bmif.exists() || bmifz.exists()) {
						continue;
					}
				}
				log.warn("----------- Get " + fileName + " " + url + " ----------");
				File toSave = downloadFile(url, fileName);
				if (toSave != null) {
					generateIndex(toSave, regionName, regionSpecificData, alreadyGeneratedFiles);
				}
			}
		}
	}

	protected File downloadFile(String url, String regionName) {
		if (!url.startsWith("http")) {
			return new File(url);
		}
		String ext = ".osm";
		if (url.endsWith(".osm.bz2")) {
			ext = ".osm.bz2";
		} else if (url.endsWith(".pbf")) {
			ext = ".osm.pbf";
		}
		File toIndex = null;
		File saveTo = new File(osmDirFiles, regionName + ext);
		if (wget == null || wget.trim().length() == 0) {
			toIndex = internalDownload(url, saveTo);
		} else {
			toIndex = wgetDownload(url, saveTo);
		}
		if (toIndex == null) {
			saveTo.delete();
		}
		return toIndex;
	}

	private File wgetDownload(String url,  File toSave)
	{
		BufferedReader wgetOutput = null;
		OutputStream wgetInput = null;
		Process wgetProc = null;
		try {
			log.info("Executing " + wget + " " + url + " -O "+ toSave.getCanonicalPath()); //$NON-NLS-1$//$NON-NLS-2$ $NON-NLS-3$
			ProcessBuilder exec = new ProcessBuilder(wget, "--read-timeout=5", "--progress=dot:binary", url, "-O", //$NON-NLS-1$//$NON-NLS-2$ $NON-NLS-3$
					toSave.getCanonicalPath());
			exec.redirectErrorStream(true);
			wgetProc = exec.start();
			wgetOutput = new BufferedReader(new InputStreamReader(wgetProc.getInputStream()));
			String line;
			while ((line = wgetOutput.readLine()) != null) {
				log.info("wget output:" + line); //$NON-NLS-1$
			}
			int exitValue = wgetProc.waitFor();
			wgetProc = null;
			if (exitValue != 0) {
				log.error("Wget exited with error code: " + exitValue); //$NON-NLS-1$
			} else {
				return toSave;
			}
		} catch (IOException e) {
			log.error("Input/output exception " + toSave.getName() + " downloading from " + url + "using wget: " + wget, e); //$NON-NLS-1$ //$NON-NLS-2$ $NON-NLS-3$
		} catch (InterruptedException e) {
			log.error("Interrupted exception " + toSave.getName() + " downloading from " + url + "using wget: " + wget, e); //$NON-NLS-1$ //$NON-NLS-2$ $NON-NLS-3$
		} finally {
			safeClose(wgetInput, ""); //$NON-NLS-1$
			safeClose(wgetOutput, ""); //$NON-NLS-1$
			if (wgetProc != null) {
				wgetProc.destroy();
			}
		}
		return null;
	}

	private final static int DOWNLOAD_DEBUG = 1 << 20;
	private final static int BUFFER_SIZE = 1 << 15;
	private static final int SLEEP_SEC = 1;
	private File internalDownload(String url, File toSave) {
		int count = 0;
		int downloaded = 0;
		int mbDownloaded = 0;
		byte[] buffer = new byte[BUFFER_SIZE];
		OutputStream ostream = null;
		InputStream stream = null;
		try {
			ostream = new FileOutputStream(toSave);
			stream = new URL(url).openStream();
			log.info("Downloading country " + toSave.getName() + " from " + url);  //$NON-NLS-1$//$NON-NLS-2$
			while ((count = stream.read(buffer)) != -1) {
				ostream.write(buffer, 0, count);
				downloaded += count;
				if(downloaded > DOWNLOAD_DEBUG){
					downloaded -= DOWNLOAD_DEBUG;
					mbDownloaded += (DOWNLOAD_DEBUG>>20);
					log.info(mbDownloaded +" megabytes downloaded of " + toSave.getName());
				}
			}
			return toSave;
		} catch (IOException e) {
			log.error("Input/output exception " + toSave.getName() + " downloading from " + url, e); //$NON-NLS-1$ //$NON-NLS-2$
		} finally {
			safeClose(ostream, "Input/output exception " + toSave.getName() + " to close stream "); //$NON-NLS-1$ //$NON-NLS-2$
			safeClose(stream, "Input/output exception " + url + " to close stream "); //$NON-NLS-1$ //$NON-NLS-2$
		}
		return null;
	}

	private static void safeClose(Closeable ostream, String message) {
		if (ostream != null) {
			try {
				ostream.close();
			} catch (Exception e) {
				log.error(message, e); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
	}

	protected void generateLocalFolderIndexes(Set<String> alreadyGeneratedFiles) {
		for (File f : getSortedFiles(osmDirFiles)) {
			if (alreadyGeneratedFiles.contains(f.getName())) {
				continue;
			}
			if (f.getName().endsWith(".osm.bz2") || f.getName().endsWith(".osm") || f.getName().endsWith(".osm.pbf")) {
				if (skipExistingIndexes != null) {
					int i = f.getName().indexOf(".osm");
					String name = Algorithms.capitalizeFirstLetterAndLowercase(f.getName().substring(0, i));
					File bmif = new File(skipExistingIndexes, name + "_" + IndexConstants.BINARY_MAP_VERSION
							+ IndexConstants.BINARY_MAP_INDEX_EXT_ZIP);
					log.info("Check if " + bmif.getAbsolutePath() + " exists");
					if (bmif.exists()) {
						continue;
					}
				}
				generateIndex(f, null, null, alreadyGeneratedFiles);
			}
		}
	}



	protected boolean generateIndex(File file, String regionName, RegionSpecificData rdata, Set<String> alreadyGeneratedFiles) {
		String fileMapName = file.getName();
		int i = file.getName().indexOf('.');
		if (i > -1) {
			fileMapName = Algorithms.capitalizeFirstLetterAndLowercase(file.getName().substring(0, i));
		}
		if (Algorithms.isEmpty(regionName)) {
			regionName = fileMapName;
		} else {
			regionName = Algorithms.capitalizeFirstLetterAndLowercase(regionName);
		}
		String targetMapFileName = fileMapName + "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_MAP_INDEX_EXT;
		for (ExternalJobDefinition jd : externalJobQueues) {
			boolean exclude = false;
			if (jd.sizeUpToMB > 0 && file.length() > jd.sizeUpToMB * 1024 * 1024) {
				exclude = true;
			} else if (jd.excludedRegions.contains(fileMapName.toLowerCase())) {
				exclude = true;
			} else {
				for (String t : jd.excludePatterns) {
					if (fileMapName.contains(t)) {
						exclude = true;
						break;
					}
				}
			}
			if (exclude) {
				continue;
			}
			if (jd.type.equals("aws")) {
				log.warn("-------------------------------------------");
				log.warn("----------- Generate on AWS " + file.getName() + " " + jd.queue +  "\n\n\n");
				try {
					generateAwsIndex(jd, file, targetMapFileName, rdata, alreadyGeneratedFiles);
					return true;
				} catch (RuntimeException e) {
					log.error("----------- FAILED Generation on AWS go to local " + file.getName() + " " + e.getMessage(), e);
				}
			} else if (jd.type.equals("docker")) {
				log.warn("-------------------------------------------");
				log.warn("----------- Generate on Docker " + file.getName() + " " + jd.queue + " \n\n\n");
				try {
					generateDockerIndex(jd, file, targetMapFileName, rdata, alreadyGeneratedFiles);
					return true;
				} catch (RuntimeException e) {
					log.error("----------- FAILED Generation on Docker " + file.getName() + " " + e.getMessage(), e);
					return false;
				}
			}
		}
		log.warn("-------------------------------------------");
		log.warn("----------- Scheduled on local " + file.getName() + "\n\n\n");
		LocalPendingGeneration lp = new LocalPendingGeneration();
		lp.file = file;
		lp.regionName = regionName;
		lp.mapFileName = targetMapFileName;
		lp.rdata = rdata;
		localPendingGenerations.add(lp);
		return false;
	}
	
	
	private void generateDockerIndex(ExternalJobDefinition jd, File file, String targetFileName,
			RegionSpecificData rdata, Set<String> alreadyGeneratedFiles) {
		String fileParam = file.getName().substring(0, file.getName().indexOf('.'));
		String currentMonth = new SimpleDateFormat("yyyy-MM").format(new Date());
		String name = MessageFormat.format(jd.name, fileParam, currentMonth, targetFileName);
//		String queue = MessageFormat.format(jd.queue, fileParam, currentMonth, targetFileName);
//		String definition = MessageFormat.format(jd.definition, fileParam, currentMonth, targetFileName);
		alreadyGeneratedFiles.add(file.getName());
		Iterator<Entry<String, String>> it = jd.params.entrySet().iterator();
		boolean srtmRun = !Algorithms.isEmpty(srtmDir) && (rdata == null || rdata.indexSRTM);
		DockerPendingGeneration p = new DockerPendingGeneration();
		p.jd = jd;
		while (it.hasNext()) {
			Entry<String, String> e = it.next();
			String vl = MessageFormat.format(e.getValue(), fileParam, currentMonth, targetFileName);
			if (e.getKey().equals("image")) {
				p.image = vl;
			} else if (e.getKey().startsWith("cmd")) {
				p.cmd.add(vl);
			} else if (e.getKey().startsWith("env")) {
				p.envs.add(vl);
			}
		}
		log.info("Submit docker request : " + name);
		
		if (p.image == null || p.cmd.isEmpty()) {
			throw new IllegalArgumentException("Can't start docker container Image or cmd is empty ");
		}
		if (srtmRun) {
			p.cmd.add("--srtm=/home/srtm");
			p.binds.add(new Bind(srtmDir, new Volume("/home/srtm")));
		}
		p.cmd.add("--upload");
		p.cmd.add("/home/result/"+targetFileName);
		p.binds.add(new Bind(indexDirFiles.getAbsolutePath(), new Volume("/home/result")));
		p.name = name;
		
		
		// to avoid concurrency issues
		jd.pendingGenerations.add(p);
	}
	
	private void generateAwsIndex(ExternalJobDefinition jd, File file, String targetFileName,
			RegionSpecificData rdata, Set<String> alreadyGeneratedFiles) {
		String fileParam = file.getName().substring(0, file.getName().indexOf('.'));
		String currentMonth = new SimpleDateFormat("yyyy-MM").format(new Date());
		String name = MessageFormat.format(jd.name, fileParam, currentMonth, targetFileName);
		String queue = MessageFormat.format(jd.queue, fileParam, currentMonth, targetFileName);
		String definition = MessageFormat.format(jd.definition, fileParam, currentMonth, targetFileName);
		alreadyGeneratedFiles.add(file.getName());
		BatchClient client = BatchClient.builder().build();
		SubmitJobRequest.Builder pr = SubmitJobRequest.builder().jobName(name).jobQueue(queue)
				.jobDefinition(definition);
		LinkedHashMap<String, String> pms = new LinkedHashMap<>();
		Iterator<Entry<String, String>> it = jd.params.entrySet().iterator();
		String uploadedS3File = null;
		while (it.hasNext()) {
			Entry<String, String> e = it.next();
			String vl = MessageFormat.format(e.getValue(), fileParam, currentMonth, targetFileName);
			pms.put(e.getKey(), vl);
			if(e.getKey().equals("upload")) {
				uploadedS3File = vl;
			}
		}
		pr.parameters(pms);
		SubmitJobRequest req = pr.build();
		log.info("Submit aws request (sleep " + SLEEP_SEC+ "s): " + req );
		AwsPendingGeneration p = new AwsPendingGeneration();
		p.response = client.submitJob(req);
		p.s3Url = uploadedS3File;
		p.targetFileName = targetFileName;
		log.info("Got response: " + p.response);
		try {
			Thread.sleep(SLEEP_SEC * 1000);
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
		}
		if (p.s3Url != null) {
			awsPendingGenerations.add(p);
		}
	}

	protected void generateLocalIndex(File file, String regionName, String mapFileName, RegionSpecificData rdata, Set<String> alreadyGeneratedFiles) {
		try {
			// reduce memory footprint for single thread generation
			// Remove it if it is called in multithread
			RTree.clearCache();

			DBDialect osmDb = this.osmDbDialect;
			if (file.length() / 1024 / 1024 > INMEM_LIMIT && osmDb == DBDialect.SQLITE_IN_MEMORY) {
				log.warn("Switching SQLITE in memory dialect to SQLITE");
				osmDb = DBDialect.SQLITE;
			}
			final boolean indAddr = indexAddress && (rdata == null || rdata.indexAddress);
			final boolean indPoi = indexPOI && (rdata == null || rdata.indexPOI);
			final boolean indTransport = indexTransport && (rdata == null || rdata.indexTransport);
			final boolean indMap = indexMap && (rdata == null || rdata.indexMap);
			final boolean indRouting = indexRouting && (rdata == null || rdata.indexRouting);
            final boolean indByProximity = indexByProximity && (rdata == null || rdata.indexByProximity);
			if(!indAddr && !indPoi && !indTransport && !indMap && !indRouting) {
				log.warn("! Skip country " + file.getName() + " because nothing to index !");
				return;
			}
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexMap = indMap;
			settings.indexAddress = indAddr;
			settings.indexPOI = indPoi;
			settings.indexTransport = indTransport;
			settings.indexRouting = indRouting;
            settings.indexByProximity = indByProximity;
			if(zoomWaySmoothness != null){
				settings.zoomWaySmoothness = zoomWaySmoothness;
			}
			boolean worldMaps = regionName.toLowerCase().contains("world") ;
			if (worldMaps) {
				if (regionName.toLowerCase().contains("basemap")) {
					return;
				}
				if (regionName.toLowerCase().contains("seamarks")) {
					// for now it's processed separately
					return;
				}
				if (regionName.toLowerCase().contains("seamarks")) {
					settings.keepOnlySeaObjects = true;
					settings.indexTransport = false;
					settings.indexAddress = false;
				}
			} else {
				if (!Algorithms.isEmpty(srtmDir) && (rdata == null || rdata.indexSRTM)) {
					settings.srtmDataFolderUrl = srtmDir;
				}
			}
			IndexCreator indexCreator = new IndexCreator(workDir, settings);
			
			indexCreator.setDialects(osmDb, osmDb);
			indexCreator.setLastModifiedDate(file.lastModified());
			indexCreator.setRegionName(regionName);
			indexCreator.setMapFileName(mapFileName);
			try {
				alreadyGeneratedFiles.add(file.getName());
				Log warningsAboutMapData = null;
				File logFileName = new File(workDir, mapFileName + GEN_LOG_EXT);
				FileHandler fh = null;
				// configure log path
				try {

					FileOutputStream fout = new FileOutputStream(logFileName);
					fout.write((new Date() + "\n").getBytes());
					fout.write((MapCreatorVersion.APP_MAP_CREATOR_FULL_NAME + "\n").getBytes());
					fout.close();
					fh = new FileHandler(logFileName.getAbsolutePath(), 10*1000*1000, 1, true);
					fh.setFormatter(new SimpleFormatter());
					fh.setLevel(Level.ALL);
					Jdk14Logger jdk14Logger = new Jdk14Logger("tempLogger");
					jdk14Logger.getLogger().setLevel(Level.ALL);
					jdk14Logger.getLogger().setUseParentHandlers(false);
					jdk14Logger.getLogger().addHandler(fh);
					warningsAboutMapData = jdk14Logger;
				} catch (SecurityException e1) {
					e1.printStackTrace();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				if (fh != null) {
					LogManager.getLogManager().getLogger("").addHandler(fh);
				}
				try {
					indexCreator.generateIndexes(file, new ConsoleProgressImplementation(1), null, mapZooms,
							new MapRenderingTypesEncoder(renderingTypesFile, file.getName()), warningsAboutMapData);
				} finally {
					if (fh != null) {
						fh.close();
						LogManager.getLogManager().getLogger("").removeHandler(fh);
					}
				}
				File generated = new File(workDir, mapFileName);
				File dest = new File(indexDirFiles, generated.getName());
				if (!generated.renameTo(dest)) {
					FileOutputStream fout = new FileOutputStream(dest);
					FileInputStream fin = new FileInputStream(generated);
					Algorithms.streamCopy(fin, fout);
					fin.close();
					fout.close();
				}
				File copyLog = new File(indexDirFiles, logFileName.getName());
				FileOutputStream fout = new FileOutputStream(copyLog);
				FileInputStream fin = new FileInputStream(logFileName);
				Algorithms.streamCopy(fin, fout);
				fin.close();
				fout.close();
				//	logFileName.renameTo(new File(indexDirFiles, logFileName.getName()));

			} catch (Exception e) {
				log.error("Exception generating indexes for " + file.getName(), e); //$NON-NLS-1$
			}
		} catch (OutOfMemoryError e) {
			System.gc();
			log.error("OutOfMemory", e);

		}
		System.gc();
	}

	protected File[] getSortedFiles(File dir){
		File[] listFiles = dir.listFiles();
		Arrays.sort(listFiles, new Comparator<File>(){
			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return listFiles;
	}
}
