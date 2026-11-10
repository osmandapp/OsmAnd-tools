package net.osmand.server.api.services;

import static net.osmand.server.api.services.UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE;
import static net.osmand.server.api.services.UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE;
import static net.osmand.server.api.services.UserdataService.INFO_EXT;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import net.osmand.IndexConstants;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.services.DownloadIndexesService.DownloadType;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;
import net.osmand.server.utils.exception.OsmAndPublicApiException;

/** Keeps obsolete standard maps out of Cloud transfers. */
@Service
public class DeletedMapsService {

	@Autowired
	private CloudUserFilesRepository filesRepository;

	private final Gson gson = new Gson();
	private final Set<String> deletedMapNames = createDeletedMapNames();

	public static void setDeletedMaps(List<DownloadIndex> deletedMaps) {
		addDeletedMap(deletedMaps, "Canada_nunavut_northamerica_2.obf.zip", DownloadType.MAP, "06.11.2026");
		addDeletedMap(deletedMaps, "Norway_vestfold-og-telemark_europe_2.obf.zip", DownloadType.MAP, "06.11.2026");
		addDeletedMap(deletedMaps, "Gb_england_channel-islands_europe_2.obf.zip", DownloadType.MAP, "06.11.2026");
		addDeletedMap(deletedMaps, "Russia_central-federal-district_asia_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Us_texas_northamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");

		addDeletedMap(deletedMaps, "Canada_nunavut_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "06.11.2026");
		addDeletedMap(deletedMaps, "Norway_vestfold-og-telemark_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "06.11.2026");
		addDeletedMap(deletedMaps, "Gb_england_channel-islands_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "06.11.2026");

		addDeletedMap(deletedMaps, "Canada_british-columbia_alberni-clayoquot_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_bulkley-nechako_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_capital-regional_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_cariboo_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_central-coast_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_central-kootenay_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_central-okanagan_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_columbia-shuswap_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_comox-valley_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_cowichan-valley_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_east-kootenay_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_fraser-fort-george_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_fraser-valley_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_greater-vancouver_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_kitimat-stikine_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_kootenay-boundary_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_mount-waddington_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_nanaimo_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_northern-rockies_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_north-okanagan_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_okanagan-similkameen_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_peace-river_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_powell-river_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_skeena-queen-charlotte_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_squamish-lillooet_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_stikine_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_strathcona_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_sunshine-coast_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_thompson-nicola_northamerica_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Gb_channel-islands_europe_2.obf.zip", DownloadType.MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Hungary_europe_2.obf.zip", DownloadType.MAP, "09.09.2026");

		addDeletedMap(deletedMaps, "Argentina_southamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Austria_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Belarus_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Belgium_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_alberni-clayoquot_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_bulkley-nechako_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_capital-regional_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_cariboo_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_central-coast_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_central-kootenay_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_central-okanagan_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_columbia-shuswap_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_comox-valley_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_cowichan-valley_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_east-kootenay_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_fraser-fort-george_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_fraser-valley_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_greater-vancouver_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_kitimat-stikine_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_kootenay-boundary_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_mount-waddington_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_nanaimo_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_northern-rockies_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_north-okanagan_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_okanagan-similkameen_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_peace-river_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_powell-river_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_skeena-queen-charlotte_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_squamish-lillooet_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_stikine_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_strathcona_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_sunshine-coast_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_thompson-nicola_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_ontario_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Canada_quebec_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Chile_southamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Czech-republic_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Denmark_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Finland_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "France_auvergne-rhone-alpes_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "France_great-east_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "France_new-aquitaine_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "France_occitania_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "France_provence-alpes-cote-d-azur_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Gb_channel-islands_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Germany_baden-wuerttemberg_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Germany_bayern_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Germany_hessen_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Germany_niedersachsen_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Hungary_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Iran_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Japan_chubu_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Japan_kanto_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Mexico_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_central-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_far-eastern-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_north-caucasus-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_northwestern-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_siberian-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_south-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_ural-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Russia_volga-federal-district_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Sweden_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Switzerland_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Tanzania_africa_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");
		addDeletedMap(deletedMaps, "Turkey_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "09.09.2026");

		addDeletedMap(deletedMaps, "Argentina_southamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Austria_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Belarus_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Belgium_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Canada_british-columbia_northamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Canada_ontario_northamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Canada_quebec_northamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Chile_southamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Congo-democratic-republic_africa_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Czech-republic_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Denmark_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Finland_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "France_auvergne-rhone-alpes_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "France_great-east_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "France_new-aquitaine_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "France_occitania_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "France_provence-alpes-cote-d-azur_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Germany_baden-wuerttemberg_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Germany_bayern_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Germany_hessen_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Germany_niedersachsen_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Iran_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Japan_chubu_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Japan_kanto_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Mexico_northamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Philippines_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Russia_far-eastern-federal-district_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Russia_north-caucasus-federal-district_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Russia_northwestern-federal-district_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Russia_siberian-federal-district_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Russia_south-federal-district_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Russia_ural-federal-district_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Russia_volga-federal-district_asia_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "South-africa_africa_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Sweden_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Switzerland_europe_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Tanzania_africa_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Us_florida_northamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Us_new-york_northamerica_2.obf.zip", DownloadType.MAP, "06.07.2026");

		addDeletedMap(deletedMaps, "Congo-democratic-republic_africa_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Philippines_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "South-africa_africa_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Us_florida_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Us_new-york_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");

		addDeletedMap(deletedMaps, "Norway_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Ukraine_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Gb_england_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Netherlands_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Us_texas_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Australia-oceania_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");
		addDeletedMap(deletedMaps, "Indonesia_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "06.07.2026");

		addDeletedMap(deletedMaps, "Turkey_europe_2.obf.zip", DownloadType.MAP, "03.04.2026");
		addDeletedMap(deletedMaps, "Us_california_northamerica_2.road.obf.zip", DownloadType.ROAD_MAP, "03.04.2026");

		addDeletedMap(deletedMaps, "Germany_nordrhein-westfalen_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "03.10.2025");
		addDeletedMap(deletedMaps, "Germany_nordrhein-westfalen_europe_2.obf.zip", DownloadType.MAP, "03.10.2025");

		addDeletedMap(deletedMaps, "India_asia_2.road.obf.zip", DownloadType.ROAD_MAP, "03.09.2025");
		addDeletedMap(deletedMaps, "Spain_europe_2.road.obf.zip", DownloadType.ROAD_MAP, "03.09.2025");
	}

	public boolean isDeletedMapFile(String name, String type) {
		if (!FILE_TYPE.equalsIgnoreCase(type)) {
			return false;
		}
		if (name.endsWith(INFO_EXT)) {
			name = name.substring(0, name.length() - INFO_EXT.length());
		}
		if (!name.endsWith(IndexConstants.BINARY_MAP_INDEX_EXT)) {
			return false;
		}
		return deletedMapNames.contains(DownloadIndexesService.toIndexFileName(name));
	}

	public void markDeletedMaps(UserFilesResults result, boolean removeFromList) {
		for (Iterator<UserFileNoData> iterator = result.uniqueFiles.iterator(); iterator.hasNext();) {
			UserFileNoData file = iterator.next();
			if (isDeletedMapFile(file.name, file.type)) {
				iterator.remove();
			}
		}
		if (result.allFiles == null) {
			return;
		}
		Set<String> marked = new HashSet<>();
		for (Iterator<UserFileNoData> iterator = result.allFiles.iterator(); iterator.hasNext();) {
			UserFileNoData file = iterator.next();
			if (isDeletedMapFile(file.name, file.type)) {
				if (removeFromList || file.name.endsWith(INFO_EXT) || !marked.add(file.type + "/" + file.name)) {
					iterator.remove();
				} else {
					// Negative size prevents downloads; zero update time prevents local deletion.
					file.filesize = -1;
					file.zipSize = 0;
					file.updatetime = new Date(0);
					file.updatetimems = 0;
				}
			}
		}
	}

	public ResponseEntity<String> handleDeletedMapUpload(String name, String type, Long clienttime, CloudUserDevice dev) {
		// Acknowledge obsolete maps without storing their content.
		long uploadTime = Math.max(System.currentTimeMillis(), clienttime == null ? 0 : clienttime);
		if (!name.endsWith(INFO_EXT)) {
			UserFile previous = filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(dev.userid, name, type);
			if (previous == null || previous.filesize == null || previous.filesize >= 0) {
				UserFile marker = new UserFile();
				marker.userid = dev.userid;
				marker.deviceid = dev.id;
				marker.name = name;
				marker.type = type;
				marker.filesize = -1L;
				marker.zipfilesize = 0L;
				marker.updatetime = new Date(uploadTime);
				marker.clienttime = clienttime == null ? null : new Date(clienttime);
				filesRepository.saveAndFlush(marker);
			}
		}
		return ResponseEntity.ok(gson.toJson(Map.of("status", "ok", "updatetime", uploadTime, "filesize", 0)));
	}

	public void validateDownload(String name, String type) {
		if (isDeletedMapFile(name, type)) {
			throw new OsmAndPublicApiException(ERROR_CODE_FILE_NOT_AVAILABLE, ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
		}
	}

	public boolean isDeletionMarker(String name, String type, long updatetime) {
		// Version cleanup must keep the synthetic deletion marker.
		return updatetime == 0 && isDeletedMapFile(name, type);
	}

	private static Set<String> createDeletedMapNames() {
		List<DownloadIndex> deletedMaps = new ArrayList<>();
		setDeletedMaps(deletedMaps);
		Set<String> names = new HashSet<>();
		for (DownloadIndex index : deletedMaps) {
			String name = index.getName();
			names.add(name.endsWith(IndexConstants.ZIP_EXT)
					? name.substring(0, name.length() - IndexConstants.ZIP_EXT.length()) : name);
		}
		return names;
	}

	private static void addDeletedMap(List<DownloadIndex> deletedMaps, String name,
			DownloadType oldType, String date) {
		try {
			DownloadIndex di = new DownloadIndex();
			di.setName(name);
			di.setDateByString(date);
			di.setType(DownloadType.DELETED_MAP);
			deletedMaps.add(di);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
