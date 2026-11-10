#!/usr/bin/env bash
# Download the free depth sources and the land mask into one folder (resumable, size-checked).
#
#   download_all.sh -o DIR [--data] [--mask] [--only src1,src2] [-j JOBS] [--dry-run]
#
# --data  depth sources into DIR/src/<name>/:
#   gebco        GEBCO_2026 elevation, 8 GeoTIFF tiles, 15"                 ~4.2 GB zip
#   gebco_tid    GEBCO_2026 type identifier grid (measured vs interpolated) ~0.1 GB zip
#   emodnet      EMODnet DTM 2024, all 58 tiles, 1/16'                      ~11.4 GB zip
#   noaa_enc     NOAA ENC, all US charts, S-57                               ~0.8 GB zip
#   cudem        NOAA CUDEM 1/3" topobathy, US coast, 373 tiles              ~8.3 GB
#   (cudem_ninth NOAA CUDEM 1/9" topobathy, 930 tiles, ~187 GB: disabled until there is disk space)
#   norway       Kartverket "Sjøkart - Dybdedata", whole country, FGDB       ~2.3 GB zip
#   netherlands  Rijkswaterstaat bottom grids 20 m 2024, Zeeland, NCP 2019   ~0.3 GB
#                then a Laplace coastal fill into src/netherlands/fill; needs mask and neighbour polygons
#   ireland      INFOMAR bathymetry 25 m (Irish waters) and 10 m (inshore), LAT, from the GSI ImageServer
#                then a Laplace coastal fill into src/ireland/fill; needs mask and great-britain.poly
#   denmark      Danmarks Dybdemodel 50 m 2024, one GeoTIFF (depths positive, mean sea level) ~0.13 GB; not in the
#                published regions: 50 m against EMODnet's 115 m is a small gain
#   france       SHOM coastal DTMs 5-20 m, chart datum (PBMA), 11 zones as GeoTIFF; then EMODnet only in holes
#                enclosed by SHOM and a Laplace fill to the coast in src/france/fill; needs emodnet and mask
#   uk           UKHO ADMIRALTY seabed surveys: BAG or CSV point delivery, whichever is finer per survey, becomes a
#                0.0002 degree GeoTIFF; with env UKHO_TOKEN (the Bearer token of a signed-in seabed.admiralty.co.uk
#                session) the surveys of the index are downloaded, zips put into DIR/src/uk/incoming by hand are
#                converted as well. Then the EMODnet underlay and uk.vrt are built and the coastal gap is
#                filled into uk_filled.vrt; needs DIR/src/emodnet/emodnet_2024.vrt, the land polygons in
#                DIR/mask and DIR/src/ireland (run --only emodnet,mask,ireland first)
#   nz           LINZ hydrographic chart vector data: depth contours, soundings, depth areas in 5 scale bands, WFS
#                into one GeoPackage; needs env LINZ_API_KEY (free LINZ Data Service key)
# --mask  OSM land polygons (osmdata.openstreetmap.de, coastline only) into DIR/mask/  ~0.9 GB zip
# Without --data and --mask both are downloaded.
#
# Archives are unzipped as soon as they are complete and then deleted; an empty <archive>.done marker keeps
# a rerun from downloading them again. Every step can be rerun: finished files are skipped, broken ones resumed.
# Germany (BSH NAUTHIS) is not included: its WFS download service is disabled (checked 2026-09-16).
# Finland (Traficom depth WFS) is not included: its licence allows non-commercial, non-navigational use only.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
OUT=""; ONLY=""; JOBS=4; DRY=0; DATA=0; MASK=0
while [ $# -gt 0 ]; do
	case "$1" in
		-o) OUT=$2; shift 2 ;;
		--data) DATA=1; shift ;;
		--mask) MASK=1; shift ;;
		--only) ONLY=$2; DATA=1; shift 2 ;;
		-j) JOBS=$2; shift 2 ;;
		--dry-run) DRY=1; shift ;;
		-h|--help) sed -n '2,33p' "$0"; exit 0 ;;
		*) echo "Unknown option $1" >&2; exit 1 ;;
	esac
done
[ -n "$OUT" ] || { echo "Give the target folder: -o DIR" >&2; exit 1; }
if [ $DATA -eq 0 ] && [ $MASK -eq 0 ]; then DATA=1; MASK=1; fi
mkdir -p "$OUT"; OUT=$(cd "$OUT" && pwd)
SRC="$OUT/src"

want() { # want NAME -> true if this source is selected
	[ $DATA -eq 1 ] || return 1
	if [ -n "$ONLY" ]; then case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac; fi
	[ "$1" != cudem_ninth ]
}
remote_size() { curl -sIL "$1" | awk 'tolower($1)=="content-length:"{v=$2} /^HTTP/{c=$2} END{gsub("\r","",v); print (c==200 ? v+0 : 0)}'; }
local_size() { if [ -f "$1" ]; then wc -c < "$1" | tr -d ' '; else echo 0; fi; }

# fetch URL FILE : resumable single-file download with size check; the download goes to FILE.part and is renamed
# when complete, so an existing FILE is complete and is not checked against the server again
fetch() {
	local url=$1 file=$2 total have
	if [ -f "$file.done" ]; then echo "ok $(basename "$file") (unzipped earlier)"; return 0; fi
	if [ -f "$file" ]; then echo "ok $(basename "$file") (downloaded earlier)"; return 0; fi
	total=$(remote_size "$url")
	if [ "$total" -le 0 ]; then echo "FAILED (no size) $url" >&2; return 1; fi
	mkdir -p "$(dirname "$file")"
	for attempt in $(seq 1 50); do
		have=$(local_size "$file.part")
		if [ "$have" -eq "$total" ]; then mv "$file.part" "$file"; echo "downloaded $(basename "$file") ($total bytes)"; return 0; fi
		if [ "$have" -gt "$total" ]; then rm -f "$file.part"; fi
		curl -sL --retry 5 --retry-delay 5 -C - -o "$file.part" "$url" || sleep 5
	done
	echo "FAILED after retries $url" >&2; return 1
}

# unzip_rm ZIP DIR : unzip a complete archive, delete it and leave ZIP.done
unzip_rm() {
	local zip=$1 dir=$2
	if [ -f "$zip.done" ]; then return 0; fi
	mkdir -p "$dir"
	unzip -oq "$zip" -d "$dir"
	rm -f "$zip"; : > "$zip.done"
	echo "unzipped and removed $(basename "$zip")"
}
# arcgis_nodata TIF : INFOMAR writes no data as 0 and, in parts of the 25 m grid, as 3.4e38; both become NaN, the
# GeoTIFF compressed. A tile whose nodata is already NaN is left as it is
arcgis_nodata() {
	local tif=$1
	if gdalinfo "$tif" 2>/dev/null | grep -q 'NoData Value=nan'; then return 0; fi
	gdalwarp -q -overwrite -srcnodata 0 -dstnodata nan "$tif" "$tif.zero.tif" \
		&& gdalwarp -q -overwrite -srcnodata 3.3999999521443642e+38 -dstnodata nan -co COMPRESS=DEFLATE -co PREDICTOR=3 \
			-co TILED=YES "$tif.zero.tif" "$tif.nan.tif" \
		&& mv "$tif.nan.tif" "$tif" && rm -f "$tif.zero.tif" \
		|| { rm -f "$tif.zero.tif" "$tif.nan.tif"; echo "FAILED nodata $(basename "$tif")" >&2; return 1; }
}
# arcgis_tile URL DIR NAME W S E N COLS ROWS : one tile of an ArcGIS ImageServer grid as a compressed GeoTIFF with
# nodata NaN (arcgis_nodata). A preview at a quarter of the cells comes first: most tiles of the extent are open ocean
# or land, those are left as NAME.empty
arcgis_tile() {
	local url=$1 dir=$2 name=$3 w=$4 s=$5 e=$6 n=$7 cols=$8 rows=$9 get
	if [ -f "$dir/$name.tif" ] || [ -f "$dir/$name.empty" ]; then return 0; fi
	get="$url/exportImage?bbox=$w,$s,$e,$n&bboxSR=4326&imageSR=4326&format=tiff&pixelType=F32"
	get+="&interpolation=RSP_NearestNeighbor&f=image"
	curl -sfL --retry 5 --retry-delay 5 -o "$dir/$name.preview" "$get&size=$((cols / 4)),$((rows / 4))" \
		|| { echo "FAILED preview $name" >&2; return 1; }
	gdal_translate -q -a_nodata 0 "$dir/$name.preview" "$dir/$name.preview.tif" || { echo "FAILED preview $name" >&2; return 1; }
	# no statistics = no cell with data
	if ! gdalinfo -stats "$dir/$name.preview.tif" 2>/dev/null | grep -q STATISTICS_MAXIMUM; then
		rm -f "$dir/$name".preview*; : > "$dir/$name.empty"; return 0
	fi
	rm -f "$dir/$name".preview*
	curl -sfL --retry 5 --retry-delay 5 -o "$dir/$name.part" "$get&size=$cols,$rows" || { echo "FAILED $name" >&2; return 1; }
	mv "$dir/$name.part" "$dir/$name.tmp.tif" && arcgis_nodata "$dir/$name.tmp.tif" \
		&& mv "$dir/$name.tmp.tif" "$dir/$name.tif" && echo "ok $name"
}

# arcgis_image URL DIR : the whole grid of an ArcGIS ImageServer at its own cell size, 4000 cells a tile
arcgis_image() {
	local url=$1 dir=$2
	mkdir -p "$dir"
	curl -sfL --retry 5 "$url?f=json" | python3 -c '
import json, math, sys
d = json.load(sys.stdin); e, px, n = d["extent"], d["pixelSizeX"], 4000
for i in range(math.ceil((e["xmax"] - e["xmin"]) / px / n)):
    for j in range(math.ceil((e["ymax"] - e["ymin"]) / px / n)):
        x0, y1 = e["xmin"] + i * n * px, e["ymax"] - j * n * px
        print("%03d_%03d %.12f %.12f %.12f %.12f %d %d" % (i, j, x0, y1 - n * px, x0 + n * px, y1, n, n))' \
	| xargs -P "$JOBS" -L 1 bash -c 'arcgis_tile "$0" "$@"' "$url" "$dir"
}
export -f fetch remote_size local_size unzip_rm arcgis_nodata arcgis_tile

report() { # dry run: print total size of URL list on stdin
	xargs -P 8 -I{} bash -c 'remote_size "$1"' _ {} | awk -v n="$1" '{s+=$1;c++} END{printf "%-12s %5d files %9.2f GB\n", n, c, s/1e9}'
}

if [ $MASK -eq 1 ]; then
	echo "== mask"
	U=https://osmdata.openstreetmap.de/download/land-polygons-complete-4326.zip
	if [ $DRY -eq 1 ]; then echo $U | report mask
	else
		# the land polygons are rebuilt daily from the OSM coastline: download again only when the server copy has
		# changed since the one unzipped here (its Last-Modified is kept next to it)
		stamp="$OUT/mask/$(basename $U).last-modified"
		remote=$(curl -sIL "$U" | awk 'tolower($1)=="last-modified:"{sub(/^[^:]*: */, ""); gsub("\r", ""); v=$0} END{print v}')
		if [ -n "$remote" ] && [ -f "$OUT/mask/$(basename $U).done" ] && [ "$remote" = "$(cat "$stamp" 2>/dev/null)" ]; then
			echo "ok $(basename $U) (unchanged since $remote)"
		else
			rm -f "$OUT/mask/$(basename $U).done" "$OUT/mask/$(basename $U).part"
			fetch $U "$OUT/mask/$(basename $U)"
			rm -rf "$OUT/mask/land-polygons-complete-4326"
			unzip_rm "$OUT/mask/$(basename $U)" "$OUT/mask"
			[ -z "$remote" ] || echo "$remote" > "$stamp"
		fi
	fi
fi
if want gebco; then
	echo "== gebco"
	if [ $DRY -eq 1 ]; then echo "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/ice_surface_elevation/geotiff/gebco_2026_geotiff.zip?download=1" | report gebco
	else "$HERE/download_gebco.sh" -y 2026 -g elevation -f geotiff -o "$SRC/gebco" --unzip; fi
fi
if want gebco_tid; then
	echo "== gebco_tid"
	if [ $DRY -eq 1 ]; then echo "https://dap.ceda.ac.uk/bodc/gebco/global/gebco_2026/type_identifier_grid/geotiff/gebco_2026_tid_geotiff.zip?download=1" | report gebco_tid
	else "$HERE/download_gebco.sh" -y 2026 -g tid -f geotiff -o "$SRC/gebco_tid" --unzip; fi
fi
if want emodnet; then
	echo "== emodnet"
	if [ $DRY -eq 1 ]; then "$HERE/download_emodnet.sh" --list | awk 'NR>1{s+=$6;c++} END{printf "%-12s %5d files %9.2f GB\n","emodnet",c,s/1000}'
	else "$HERE/download_emodnet.sh" --all -j "$JOBS" -o "$SRC/emodnet" --unzip; fi
fi
if want noaa_enc; then
	echo "== noaa_enc"
	U=https://charts.noaa.gov/ENCs/All_ENCs.zip
	if [ $DRY -eq 1 ]; then echo $U | report noaa_enc
	else fetch $U "$SRC/noaa_enc/All_ENCs.zip"; unzip_rm "$SRC/noaa_enc/All_ENCs.zip" "$SRC/noaa_enc"; fi
fi
cudem() { # cudem NAME DATASET_DIR LIST_ID
	local name=$1 dir=$2 id=$3 list
	echo "== $name"
	mkdir -p "$SRC/$name"; list="$SRC/$name/urllist$id.txt"
	curl -sL --retry 5 -o "$list" "https://coast.noaa.gov/htdata/raster2/elevation/$dir/urllist$id.txt"
	if [ $DRY -eq 1 ]; then grep -E '\.tif$' "$list" | report "$name"; return; fi
	grep -E '\.tif$' "$list" | xargs -P "$JOBS" -I{} bash -c 'fetch "$1" "$2/$(basename "$1")"' _ {} "$SRC/$name/tiles"
}
if want cudem; then cudem cudem NCEI_third_Topobathy_2014_8580 8580; fi
# disabled until there is disk space for ~187 GB
# if want cudem_ninth; then cudem cudem_ninth NCEI_ninth_Topobathy_2014_8483 8483; fi
if want norway; then
	echo "== norway"
	U=https://nedlasting.geonorge.no/geonorge/Basisdata/Dybdedata/FGDB/Basisdata_0000_Norge_4258_Dybdedata_FGDB.zip
	if [ $DRY -eq 1 ]; then echo $U | report norway
	else fetch $U "$SRC/norway/$(basename $U)"; unzip_rm "$SRC/norway/$(basename $U)" "$SRC/norway"; fi
fi
if want netherlands; then
	echo "== netherlands"
	R=https://downloads.rijkswaterstaatdata.nl
	NL="$R/bodemhoogte_20mtr/bodemhoogte_20mtr_2024.tif $R/bodemhoogte_zeeland/bodemhoogte_zeeland.tif $R/bathymetrie_ncp/bathymetrie_ncp_juni_2019.tif"
	# NSGI hydroid (LAT) and NAP geoid of PROJ: their difference is how far chart datum lies below NAP
	NL="$NL https://cdn.proj.org/nl_nsgi_nllat2018.tif https://cdn.proj.org/nl_nsgi_nlgeo2018.tif"
	if [ $DRY -eq 1 ]; then printf "%s\n" $NL | report netherlands
	else
		for u in $NL; do fetch "$u" "$SRC/netherlands/$(basename "$u")"; done
		# The coastal gap between the Rijkswaterstaat grids and the shore is filled by Laplace, like UK and
		# France. The working grid is 0.0002 degrees in chart datum: the NCP 2019 is LAT already, the
		# bodemhoogte 2024 is NAP, so the NSGI geoids bring it to LAT (depth + nlgeo2018 - nllat2018).
		land="$OUT/mask/land_polygons.gpkg"
		[ -f "$land" ] || land=$(find "$OUT/mask" -name 'land_polygons.shp' -print -quit 2>/dev/null)
		POLY_DIR=${POLY_DIR:-/home/xmd5a/git/OsmAnd-misc/osm-planet/polygons/europe/west-europe}
		if [ -n "$land" ] && [ -e "$POLY_DIR/belgium.poly" ] && [ -e "$POLY_DIR/germany.poly" ]; then
			NL_FILL="$SRC/netherlands/fill"; mkdir -p "$NL_FILL"
			for spec in "bathymetrie_ncp_juni_2019.tif:ncp_lat.tif:bilinear" \
			            "bodemhoogte_20mtr_2024.tif:bodem_nap.tif:average" \
			            "nl_nsgi_nlgeo2018.tif:nl_nsgi_nlgeo2018.tif:bilinear" \
			            "nl_nsgi_nllat2018.tif:nl_nsgi_nllat2018.tif:bilinear"; do
				src=${spec%%:*}; rest=${spec#*:}; dst=${rest%%:*}; res=${rest#*:}
				gdalwarp -q -overwrite -t_srs EPSG:4326 -te 1.7 51.1 7.3 55.7 -tr 0.0002 0.0002 \
					-r "$res" -ot Float32 -dstnodata nan -multi -wo NUM_THREADS="$JOBS" \
					-co COMPRESS=DEFLATE -co PREDICTOR=3 -co TILED=YES -co BIGTIFF=IF_SAFER \
					"$SRC/netherlands/$src" "$NL_FILL/$dst"
			done
			read -r NW NH <<< "$(gdalinfo -json "$NL_FILL/bodem_nap.tif" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(*d["size"])')"
			GT=$(gdalinfo -json "$NL_FILL/bodem_nap.tif" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(",".join(map(repr, d["geoTransform"])))')
			cat > "$NL_FILL/bodem_lat.vrt" <<-VRT
			<VRTDataset rasterXSize="$NW" rasterYSize="$NH"><SRS>EPSG:4326</SRS><GeoTransform>$GT</GeoTransform>
			<VRTRasterBand dataType="Float32" band="1" subClass="VRTDerivedRasterBand"><NoDataValue>nan</NoDataValue>
			<PixelFunctionType>sum</PixelFunctionType><PixelFunctionArguments propagateNoData="true"/>
			<SimpleSource><SourceFilename relativeToVRT="1">bodem_nap.tif</SourceFilename><SourceBand>1</SourceBand></SimpleSource>
			<SimpleSource><SourceFilename relativeToVRT="1">nl_nsgi_nlgeo2018.tif</SourceFilename><SourceBand>1</SourceBand></SimpleSource>
			<ComplexSource><SourceFilename relativeToVRT="1">nl_nsgi_nllat2018.tif</SourceFilename><SourceBand>1</SourceBand><ScaleRatio>-1</ScaleRatio></ComplexSource>
			</VRTRasterBand></VRTDataset>
			VRT
			gdalbuildvrt -q -overwrite -resolution user -tr 0.0002 0.0002 -te 1.7 51.1 7.3 55.7 \
				-srcnodata nan -vrtnodata nan "$NL_FILL/netherlands_base.vrt" \
				"$NL_FILL/ncp_lat.tif" "$NL_FILL/bodem_lat.vrt"
			NETHERLANDS_COASTAL_FILL=${NETHERLANDS_COASTAL_FILL:-6000}
			"$HERE/uk_coastal_fill.py" "$NL_FILL/netherlands_base.vrt" "$NL_FILL" --land "$land" \
				--foreign "$POLY_DIR/belgium.poly" --foreign "$POLY_DIR/germany.poly" \
				--foreign "$POLY_DIR/france.poly" --foreign "$POLY_DIR/great-britain.poly" \
				--limit "$NETHERLANDS_COASTAL_FILL" --name netherlands_filled -j "$JOBS" --force
			# the EMODnet base of the map: only inside the contour of the detailed data
			"$HERE/depth_emodnet_base.py" --name netherlands \
				--detail "$NL/bathymetrie_ncp_juni_2019.tif" "$NL/bodemhoogte_20mtr_2024.tif" \
				--emodnet "$SRC/emodnet/emodnet_2024.vrt" --bbox 1.7 51.1 7.3 55.7 \
				--out "$NL_FILL/netherlands_emodnet_base.tif" \
				--region "$SRC/netherlands/netherlands_emodnet_region.gpkg"
		else
			echo "netherlands: coastal fill skipped (no land polygons or neighbour polygons; run --mask first)" >&2
		fi
	fi
fi
if want ireland; then
	echo "== ireland"
	B=https://gsi.geodata.gov.ie/imagehost/rest/services/Marine
	if [ $DRY -eq 1 ]; then echo "ireland      ArcGIS ImageServer tiles, size known after the download"
	else
		arcgis_image "$B/IE_GSI_MI_Bathymetry_25m_IE_Waters_WGS84_LAT_GRID/ImageServer" "$SRC/ireland/25m"
		arcgis_image "$B/IE_GSI_MI_Bathymetry_10m_Inshore_IE_WGS84_LAT_GRID/ImageServer" "$SRC/ireland/10m"
		# tiles downloaded before the 3.4e38 fix
		find "$SRC/ireland/25m" "$SRC/ireland/10m" -name '*.tif' ! -name '*.tmp.tif' -print0 \
			| xargs -0 -P "$JOBS" -n 1 bash -c 'arcgis_nodata "$0"'
		# INFOMAR tiles: 25 m first, the 10 m inshore grid on top (later files win in a VRT)
		IE_FILES=()
		while IFS= read -r f; do IE_FILES+=("$f"); done < <(find "$SRC/ireland/25m" -name '*.tif' 2>/dev/null | sort; find "$SRC/ireland/10m" -name '*.tif' 2>/dev/null | sort)
		if [ ${#IE_FILES[@]} -gt 0 ]; then
			gdalbuildvrt -q -overwrite -resolution highest "$SRC/ireland/ireland.vrt" "${IE_FILES[@]}"
			# the coastal gap between INFOMAR and the shore is filled by Laplace, like UK and France;
			# the working grid is the 0.0002 degree grid of Ireland_contours
			land="$OUT/mask/land_polygons.gpkg"
			[ -f "$land" ] || land=$(find "$OUT/mask" -name 'land_polygons.shp' -print -quit 2>/dev/null)
			POLY_DIR=${POLY_DIR:-/home/xmd5a/git/OsmAnd-misc/osm-planet/polygons/europe/west-europe}
			if [ -n "$land" ] && [ -e "$POLY_DIR/great-britain.poly" ]; then
				IE_FILL="$SRC/ireland/fill"; mkdir -p "$IE_FILL"
				gdalwarp -q -overwrite -t_srs EPSG:4326 -te -11.8 51.2 -5.3 55.6 -tr 0.0002 0.0002 \
					-r average -ot Float32 -dstnodata nan -multi -wo NUM_THREADS="$JOBS" \
					-co COMPRESS=DEFLATE -co PREDICTOR=3 -co TILED=YES -co BIGTIFF=IF_SAFER \
					"$SRC/ireland/ireland.vrt" "$IE_FILL/ireland_base.tif"
				IRELAND_COASTAL_FILL=${IRELAND_COASTAL_FILL:-6000}
				"$HERE/uk_coastal_fill.py" "$IE_FILL/ireland_base.tif" "$IE_FILL" --land "$land" \
					--foreign "$POLY_DIR/great-britain.poly" --limit "$IRELAND_COASTAL_FILL" \
					--name ireland_filled -j "$JOBS" --force
				# the EMODnet base of the map: only inside the contour of the detailed data
				"$HERE/depth_emodnet_base.py" --name ireland --detail "$SRC/ireland/ireland.vrt" \
					--emodnet "$SRC/emodnet/emodnet_2024.vrt" --bbox -11.8 51.2 -5.3 55.6 \
					--out "$IE_FILL/ireland_emodnet_base.tif" \
					--region "$SRC/ireland/ireland_emodnet_region.gpkg"
			else
				echo "ireland: coastal fill skipped (no land polygons or great-britain.poly; run --mask first)" >&2
			fi
		fi
	fi
fi
if want denmark; then
	echo "== denmark"
	# the public weblink behind "Gå til download" on dataforsyningen.dk/data/4817, no login needed
	U="https://ftp.sdfe.dk/main.html?download&weblink=b4324b6389898704fd8ec7484882dbf3&subfolder=2024&realfilename=ddm%5F50m%2Edybde%2Etiff"
	if [ $DRY -eq 1 ]; then echo "$U" | report denmark
	else fetch "$U" "$SRC/denmark/ddm_50m.dybde.tiff"; fi
fi
if want france; then
	echo "== france"
	B=https://services.data.shom.fr/INSPIRE/telechargement/prepackageGroup
	# GROUP/PACKAGE of the SHOM coastal DTMs in chart datum (PBMA); the package names in their ISO metadata are
	# sometimes wrong, these are the ones the download service lists
	FR="MNT_COTIER_DETROIT_PDC_20m_TANDEM_PACK_DL/MNT_COTIER_DETROIT_PAS-DE-CALAIS_TANDEM_PBMA
MNT_COTIER_MORBIHAN_TANDEM_20m_PBMA_4326_PACK_DL/MNT_COTIER_MORBIHAN_TANDEM_PBMA
MNT_COTIER_GIRONDE_AMONT_20m_PACK_DL/MNT_COTIER_ESTUAIRE_GIRONDE_AMONT_HOMONIM_PBMA
MNT_COTIER_GIRONDE_AVAL_20m_PACK_DL/MNT_COTIER_ESTUAIRE_GIRONDE_AVAL_HOMONIM_PBMA
MNT_COTIER_ILE_DE_RE_5m_PBMA_PACK_DL/MNT_COTIER_ILE_DE_RE_HOMONIM_PBMA
MNT_COTIER_PORT_SM_PAPI_SM_5m_PACK_DL/MNT_COTIER_PORT_SAINT-MALO_PAPI_PBMA
MNT_COTIER_PORT_BSM_TANDEM_10m_PBMA_4326_PACK_DL/MNT_COTIER_PORT_BSM_TANDEM_PBMA
MNT_COTIER_BAIE_SJL_TANDEM_20m_PACK_DL/MNT_COTIER_BAIE_SAINT_JEAN_DE_LUZ_TANDEM_PBMA
MNT_COTIER_PERTUIS_HOMONIM_20m_PBMA_4326_PACK_DL/MNT_COTIER_PERTUIS_HOMONIM_PBMA
MNT_COTIER_ARCACHON_HOMONIM_20m_PACK_DL/MNT_COTIER_ARCACHON_HOMONIM_PBMA
MNT_COTIER_GNB_PAPI_SM_20m_PACK_DL/MNT_COTIER_GOLFE_NORMAND_BRETON_PAPI_PBMA"
	if [ $DRY -eq 1 ]; then for gp in $FR; do echo "$B/${gp%%/*}/prepackage/${gp#*/}/file/${gp#*/}.7z"; done | report france
	else
		mkdir -p "$SRC/france"
		for gp in $FR; do
			pkg=${gp#*/}; tif="$SRC/france/$pkg.tif"
			if [ -f "$tif" ]; then echo "ok $pkg.tif"; continue; fi
			fetch "$B/${gp%%/*}/prepackage/$pkg/file/$pkg.7z" "$SRC/france/$pkg.7z"
			# the ESRI ASCII grid: exact 0.0002 degree cells, but no CRS in the file (WGS84 per the metadata)
			rm -rf "$SRC/france/$pkg.x"; mkdir -p "$SRC/france/$pkg.x"
			if command -v 7z >/dev/null; then 7z x -bd -y -o"$SRC/france/$pkg.x" "$SRC/france/$pkg.7z" '*.asc' -r >/dev/null
			else bsdtar -xf "$SRC/france/$pkg.7z" -C "$SRC/france/$pkg.x" --include '*.asc'; fi
			asc=$(find "$SRC/france/$pkg.x" -name '*.asc' | head -1)
			[ -n "$asc" ] || { echo "FAILED france: no .asc in $pkg.7z" >&2; exit 1; }
			gdal_translate -q -a_srs EPSG:4326 -co COMPRESS=DEFLATE -co PREDICTOR=3 -co TILED=YES "$asc" "$tif.tmp.tif" \
				&& mv "$tif.tmp.tif" "$tif" && rm -rf "$SRC/france/$pkg.x" "$SRC/france/$pkg.7z" && echo "ok $pkg.tif"
		done
		gdalbuildvrt -q -overwrite -resolution highest "$SRC/france/france.vrt" "$SRC"/france/*.tif
		# EMODnet closes only holes enclosed by SHOM; the coastal strip is interpolated without an EMODnet band
		FRANCE_COASTAL_FILL=${FRANCE_COASTAL_FILL:-6000}
		POLY_DIR=${POLY_DIR:-/home/xmd5a/git/OsmAnd-misc/osm-planet/polygons/europe}
		FRANCE_FOREIGN_POLYS=${FRANCE_FOREIGN_POLYS:-"$POLY_DIR/west-europe/belgium.poly $POLY_DIR/west-europe/netherlands.poly $POLY_DIR/south-europe/spain.poly"}
		emodnet="$SRC/emodnet/emodnet_2024.vrt"
		land="$OUT/mask/land_polygons.gpkg"
		[ -f "$land" ] || land=$(find "$OUT/mask" -name 'land_polygons.shp' -print -quit 2>/dev/null)
		for need in "$emodnet" $FRANCE_FOREIGN_POLYS; do
			[ -e "$need" ] || { echo "FAILED france: $need not found (EMODnet and the neighbour polygons are required)" >&2; exit 1; }
		done
		[ -n "$land" ] || { echo "FAILED france: no land polygons in $OUT/mask (run --mask first)" >&2; exit 1; }
		foreign_args=()
		for p in $FRANCE_FOREIGN_POLYS; do foreign_args+=(--foreign "$p"); done
		# great-britain.poly is deliberately not used: its Channel boundary also covers parts of the French coast.
		# Mask the actual UK raster instead; build_depth_region.sh also cuts its polygons out of the depth areas.
		exclude_args=()
		if [ -f "$SRC/uk/uk_filled.vrt" ]; then exclude_args=(--exclude-raster "$SRC/uk/uk_filled.vrt")
		elif [ -f "$SRC/uk/uk.vrt" ]; then exclude_args=(--exclude-raster "$SRC/uk/uk.vrt"); fi
		"$HERE/france_emodnet_fill.py" "$SRC/france/france.vrt" "$emodnet" "$SRC/france/fill" \
			--land "$land" "${foreign_args[@]}" -j "$JOBS"
		"$HERE/uk_coastal_fill.py" "$SRC/france/fill/france_with_emodnet.vrt" "$SRC/france/fill" \
			--land "$land" --land-clip "$SRC/france/fill/land_clip.gpkg" "${foreign_args[@]}" "${exclude_args[@]}" \
			--limit "$FRANCE_COASTAL_FILL" --name france_filled -j "$JOBS"
	fi
fi
if want uk; then
	echo "== uk"
	# UKHO ADMIRALTY seabed surveys. Both deliveries are used: the CSV point set when its spacing is at least as fine
	# as the BAG, the BAG otherwise (uk_zip_to_tif.py picks per survey). Depths are negative, LAT; a survey becomes a
	# 0.0002 degree GeoTIFF, so a 78 MB 2 m BAG turns into a 1.7 MB file. The surveys are then blended into uk.vrt
	# over the EMODnet coastal underlay, with 10 px transitions between surveys and 20 px to the underlay.
	UKHO_CONV_JOBS=${UKHO_CONV_JOBS:-$JOBS}
	UK_FEATHER=${UK_FEATHER:-5}
	UK_COASTAL_FILL=${UK_COASTAL_FILL:-6000}
	# OsmAnd-misc polygon of Ireland: Irish waters belong to Ireland_contours and are cut out of the UK grid
	IRELAND_POLY=${IRELAND_POLY:-/home/xmd5a/git/OsmAnd-misc/osm-planet/polygons/europe/west-europe/ireland.poly}
	# the neighbouring countries whose land lies in the UK grid: it is not the coast of this map,
	# so it stays NaN and the coastal fill does not reach into their waters
	POLY_DIR=${POLY_DIR:-/home/xmd5a/git/OsmAnd-misc/osm-planet/polygons/europe/west-europe}
	UK_FOREIGN_POLYS=${UK_FOREIGN_POLYS:-"$POLY_DIR/france.poly $POLY_DIR/belgium.poly $POLY_DIR/netherlands.poly $POLY_DIR/ireland.poly"}
	if [ $DRY -eq 1 ]; then
		echo "uk           UKHO Seabed Mapping Service surveys (BAG or CSV points), then the EMODnet underlay, uk.vrt and the coastal fill"
	else
		mkdir -p "$SRC/uk/incoming" "$SRC/uk/grid" "$SRC/uk/downloaded"
		uk_args=(-o "$SRC/uk" -j "$UKHO_CONV_JOBS")
		# the zips downloaded earlier are an archive: take them from there and never delete them
		[ ! -d "$SRC/uk/incoming_full" ] || uk_args+=(--zip-dir "$SRC/uk/incoming_full")
		# download and convert; without UKHO_TOKEN only the zips already there are converted
		"$HERE/download_ukho.py" "${uk_args[@]}"
		# zips of surveys the index does not know (put there by hand) are converted here and then removed
		for z in "$SRC"/uk/incoming/*.zip; do
			[ -f "$z" ] || continue
			out=$("$HERE/uk_zip_to_tif.py" "$z" "$SRC/uk")
			printf '%s\n' "$out"
			if printf '%s\n' "$out" | grep -q '^tif='; then rm -f "$z"
			else echo "FAILED uk: $(basename "$z"), zip kept in incoming" >&2; fi
		done
		ireland="$SRC/ireland/ireland.vrt"
		emodnet="$SRC/emodnet/emodnet_2024.vrt"
		land="$OUT/mask/land_polygons.gpkg"
		[ -f "$land" ] || land=$(find "$OUT/mask" -name 'land_polygons.shp' -print -quit 2>/dev/null)
		for need in "$ireland" "$emodnet" "$IRELAND_POLY"; do
			[ -e "$need" ] || { echo "FAILED uk: $need not found (run --only emodnet,mask,ireland first)" >&2; exit 1; }
		done
		[ -n "$land" ] || { echo "FAILED uk: no land polygons in $OUT/mask (run --mask first)" >&2; exit 1; }
		# the land of the neighbouring countries is not zeroed and their waters are not filled;
		# the EMODnet underlay also stops at the midpoint between British and neighbouring land
		foreign_args=()
		for p in $UK_FOREIGN_POLYS; do
			[ -e "$p" ] || { echo "FAILED uk: $p not found (the polygons of the neighbouring countries)" >&2; exit 1; }
			foreign_args+=(--foreign "$p")
		done
		# lattice holes and thin edge notches of the point surveys
		"$HERE/uk_fill_holes.py" "$SRC/uk" -j "$UKHO_CONV_JOBS"
		# Irish waters are INFOMAR, not UKHO: mask them and move empty surveys out of grid
		"$HERE/uk_exclude_ireland.py" "$SRC/uk" "$ireland" "$IRELAND_POLY" -j "$UKHO_CONV_JOBS" --apply
		# the EMODnet underlay of the coastal gaps, then the blended mosaic (uk.vrt)
		"$HERE/uk_emodnet_fill.py" "$SRC/uk" "$emodnet" "$land" "$ireland" "$IRELAND_POLY" \
			--output "$OUT/preview" "${foreign_args[@]}" --build-vrt
		"$HERE/uk_build_vrt.py" "$SRC/uk" --feather "$UK_FEATHER" -j "$UKHO_CONV_JOBS"
		# fill the gap between the coastline (0 m) and the surveys, so depth areas reach the coast
		"$HERE/uk_coastal_fill.py" "$SRC/uk/uk.vrt" "$SRC/uk" --land "$land" \
			--exclude "$IRELAND_POLY" --exclude-raster "$ireland" "${foreign_args[@]}" \
			--limit "$UK_COASTAL_FILL" -j "$UKHO_CONV_JOBS" --force
		# the EMODnet base of the map: only inside the contour of the detailed data, and only
		# up to the midpoint to the neighbouring land
		"$HERE/depth_emodnet_base.py" --name uk --detail "$SRC/uk/uk_detail.vrt" \
			--emodnet "$emodnet" --bbox -8.7 49.8 2.0 60.9 \
			--allowed "$SRC/uk/uk_home_water.gpkg" \
			--out "$SRC/uk/emodnet_base.tif" --region "$SRC/uk/uk_emodnet_region.gpkg"
	fi
fi
if want nz; then
	echo "== nz"
	if [ -z "${LINZ_API_KEY:-}" ]; then echo "FAILED nz: set LINZ_API_KEY" >&2
	elif [ $DRY -eq 1 ]; then echo "nz           LINZ WFS, 15 layers, about 0.3 GB"
	else
		mkdir -p "$SRC/nz"; rm -f "$SRC/nz/linz_hydro.tmp.gpkg"
		# band 1 is the largest scale (harbour charts), 5 the smallest; every band: contours, soundings, depth areas
		b=0
		for layers in "50672 50858 50671" "50554 50866 50553" "50448 50506 50447" "50849 50418 50852" "51638 51612 51639"; do
			b=$((b + 1)); read -r contour sounding area <<< "$layers"
			for pair in "contour:$contour" "sounding:$sounding" "area:$area"; do
				ogr2ogr -f GPKG -update -append "$SRC/nz/linz_hydro.tmp.gpkg" "WFS:https://data.linz.govt.nz/services;key=$LINZ_API_KEY/wfs" \
					"layer-${pair#*:}" -nln "${pair%%:*}_$b" -oo EXPOSE_GML_ID=NO \
					--config OGR_WFS_PAGING_ALLOWED ON --config OGR_WFS_PAGE_SIZE 10000 \
					|| { echo "FAILED nz layer-${pair#*:}" >&2; exit 1; }
				echo "ok ${pair%%:*}_$b (layer-${pair#*:})"
			done
		done
		mv "$SRC/nz/linz_hydro.tmp.gpkg" "$SRC/nz/linz_hydro.gpkg"
	fi
fi
echo "Done: $OUT"
