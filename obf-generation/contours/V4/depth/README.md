# Depth source grids

Download scripts for the grids used to rebuild depth contours and depth points.
Both scripts resume after a dropped connection and check the final size, so just rerun them.

What the published depth OBFs are built from today (checked 2026-09-16 by sampling the grids at contour vertices):

| OBF | Source |
|---|---|
| `World_contours`, `World_*_hemisphere_points` | GEBCO One Minute Grid 2008 (1′) |
| `Europe_contours`, `Europe_points` | EMODnet DTM 2016 (1/8′) |

Current releases: GEBCO_2026 (15″) and EMODnet DTM 2024 (1/16′).

## Everything at once

`download_all.sh -o DIR` fetches all sources below into `DIR/<source>/`; rerun to resume.
Jenkins: `SRTM_DownloadDepthSources` (parameters OUT_DIR, SOURCES, JOBS, UNZIP, DRY_RUN).

```
./download_all.sh -o /home/relief-data/depth-sources --dry-run      # sizes only
./download_all.sh -o /home/relief-data/depth-sources --unzip
./download_all.sh -o /data/depth --only emodnet,netherlands -j 4
```

| Source | What | Download (2026-09-16) |
|---|---|---|
| `gebco` | GEBCO_2026 elevation, 15″, 8 GeoTIFF tiles | 4.24 GB (7.5 GB unzipped) |
| `gebco_tid` | GEBCO_2026 type identifier grid | 0.10 GB (3.4 GB unzipped) |
| `emodnet` | EMODnet DTM 2024, 1/16′, 58 tiles | 11.43 GB |
| `noaa_enc` | NOAA ENC, all US charts, S-57 | 0.83 GB |
| `cudem` | NOAA CUDEM 1/3″ topobathy, 373 tiles | 8.25 GB |
| `cudem_ninth` | NOAA CUDEM 1/9″, 930 tiles, only with `--cudem-ninth` or in `--only` | 187.38 GB |
| `norway` | Kartverket "Sjøkart - Dybdedata", FGDB, open (Geonorge "no restrictions") | 2.33 GB |
| `netherlands` | Rijkswaterstaat bottom height 20 m 2024, Zeeland, NCP 2019, CC0 | 0.26 GB |

Not scriptable: Kartverket ENC (sold via PRIMAR), BSH NAUTHIS (WFS download disabled).

## GEBCO

```
./download_gebco.sh -o /data/gebco --unzip                 # GEBCO_2026 elevation, 8 GeoTIFF tiles (~4.2 GB zip)
./download_gebco.sh -g tid -o /data/gebco --unzip          # type identifier grid (~92 MB zip)
./download_gebco.sh -y 2026 -f netcdf -j 8 -o /data/gebco  # single netCDF instead of tiles
```

## EMODnet DTM

```
./download_emodnet.sh --list                               # tiles with bounds and zip sizes
./download_emodnet.sh D5 D6 E6 -o /data/emodnet --unzip    # pilot: North Sea, Baltic, Adriatic
./download_emodnet.sh --bbox 3 51 9 56 -o /data/emodnet    # all tiles touching a box (W S E N)
./download_emodnet.sh --all -j 4 -o /data/emodnet          # all 58 tiles, ~11.4 GB zipped GeoTIFF
```
