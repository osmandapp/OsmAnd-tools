# Depth source licences

The licence of every depth source that the depth OBFs are built from or that was checked for them (2026-09-17).
OsmAnd is a commercial app used on the water, so every entry answers two questions: is commercial use allowed, and
what does the source say about navigation. Almost every open source says "not for navigation"; that is a disclaimer
the depth maps already carry, not a ban on distributing them. A source that allows only non-commercial use is not
used.

## Used

**GEBCO_2026 grid** (`gebco`, `gebco_tid`) - https://www.gebco.net/data-products/gridded-bathymetry-data
Public domain, free of charge, commercial use included. Attribution: "GEBCO Compilation Group (2026) GEBCO_2026
Grid". The terms of use disclaim any fitness for navigation.

**EMODnet Digital Bathymetry DTM 2024** (`emodnet`) -
https://emodnet.ec.europa.eu/en/terms-use-emodnet-online-services-data-and-data-products
CC BY 4.0, owned by the EU, commercial use allowed with attribution ("EMODnet Bathymetry Consortium (2024): EMODnet
Digital Bathymetry (DTM 2024)"). Metadata constraint: "DO NOT USE FOR NAVIGATION".

**NOAA ENC** (`noaa_enc`) - https://charts.noaa.gov/ENCs/ENC_Agreement.shtml
US government work in the public domain, may be redistributed and converted. A converted or redistributed ENC is not
an official NOAA ENC and does not satisfy chart carriage rules (15 CFR 995) unless the distributor is certified.

**NOAA NCEI CUDEM** (`cudem`) -
https://www.ncei.noaa.gov/metadata/geoportal/rest/metadata/item/gov.noaa.ngdc.mgg.dem:799913/html
Produced by NOAA NCEI, not subject to copyright protection in the US; no licence restriction. Use limitation: "Not to
be used for navigation"; cite CIRES / NCEI.

**Kartverket "Sjøkart - Dybdedata"** (`norway`) -
https://data.norge.no/en/datasets/946d865a-e9ac-439e-8cbf-15de7ada9a3f/sjokart-dybdedata
CC BY 4.0, public access, commercial use allowed with attribution to Kartverket. Depths refer to chart datum.

**Rijkswaterstaat bottom height 20 m, Zeeland, NCP 2019** (`netherlands`) -
https://data.overheid.nl/en/dataset/61446-bathymetrie-nederland---kust-en-vaklodingen-20-mtr
CC0 1.0: no conditions at all. Heights refer to NAP, not chart datum.

**NSGI NLLAT2018 / NLGEO2018 in PROJ-data** (`netherlands`, used to shift NAP to LAT) - https://cdn.proj.org/
CC BY 4.0, Netherlands Partnership Geodetic Infrastructure (NSGI), as written in the TIFF copyright tag.

**OSM land polygons** (`mask`) - https://osmdata.openstreetmap.de/data/land-polygons.html
ODbL 1.0, © OpenStreetMap contributors. Used only to cut land out, as the rest of OsmAnd's OSM data.

**INFOMAR bathymetry 10 m and 25 m** (`ireland`) - https://www.infomar.ie/data
CC BY 4.0, commercial use allowed. Attribution: "Contains Irish Public Sector Data (Geological Survey Ireland & Marine
Institute) licensed under a Creative Commons Attribution 4.0 International (CC BY 4.0) license". Depths refer to LAT.

## Checked, not used

**Danmarks Dybdemodel 50 m** - https://gst.dk/ansvarsomraader/soekort-og-marine-data/soeopmaaling-og-dybdedata/danmarks-dybdemodel
Licence fits: free, worldwide, commercial use allowed ("bruges kommercielt og ikke-kommercielt"). Conditions: not
for navigation, no implied endorsement, credit "Indeholder data fra Geodatastyrelsen, Danmarks Dybdemodel, 50 m
opløsning", the download date and "Model og data er ikke egnet til navigation". Not scripted because Dataforsyningen
downloads need a login. Depths refer to mean sea level (DKSML 2022), not chart datum.

**Traficom (Finland) depth contours, areas and soundings** -
https://traficom.fi/fi/ajankohtaista/paikkatietoaineistot/merikartoitusaineistot
Not usable. The depth layers are only in the restricted WFS, whose licence allows use "for other than navigational
purposes" and modification "non-commercially"; navigation use needs a separate agreement (copyright@traficom.fi).

**LINZ hydrographic chart vector data (New Zealand)** - https://data.linz.govt.nz/ (layers 50672, 50858, 50671 and
the other scale bands)
CC BY 4.0, commercial use allowed. Attribution "Contains data sourced from the LINZ Data Service and licensed for
reuse under CC BY 4.0". The metadata says the data does not replace official ENCs, should not be used for navigation
and is not corrected for Notices to Mariners. Depths refer to approximately LAT. Not scripted yet: WFS and exports
need a free LINZ API key. The layers were last published 2023-04-13; LINZ paused the chart vector updates in 2024.

**AusSeabed / Geoscience Australia 30 m grids: Torres Strait 2023, Great Barrier Reef 2020** -
https://files.ausseabed.gov.au/survey/ (DOI 10.26186/144348 for Torres Strait)
CC BY 4.0 in the GA metadata, commercial use allowed, "Not to be used for navigation". But both grids contain
Australian Hydrographic Office surveys: the GBR grid says that material "may not be copied, reproduced ... without the
prior written consent of the Australian Hydrographic Service", and the Torres Strait source list marks 2 surveys "not
to be distributed". Needs written confirmation from GA/AHO before depth maps derived from it are shipped. Open
download, no login, 5.3 GB of Cloud-Optimized GeoTIFFs; heights approximate MSL, not chart datum.

**CHS NONNA-10 / NONNA-100 (Canada)** - https://data.chs-shc.ca/ , licence
https://api-proxy.edh-cde.dfo-mpo.gc.ca/catalogue/records/d3881c4c-650d-4070-bf9b-1e00aabf0a1d/attachments/CHS_NONNA_LICENCE-LICENCE_NONNA_DU_SHC.pdf
The catalogue record says OGL-Canada, but the data comes under the stricter CHS NONNA Licence: the data "shall not be
used for navigation ... nor will any product created by the User from the Data" (§3), must not be sold or given to
a third party as such (§2), every derived product must carry a fixed CHS notice (§7), and CHS may end the licence by
public notice, after which all copies are destroyed within 15 days (§12, §14). Commercial use is not forbidden, but
§3 conflicts with a nautical map; needs a legal read. Open WCS without a key; chart datum, negative below it.

**UKHO ADMIRALTY seabed mapping surveys (United Kingdom)** - https://seabed.admiralty.co.uk/
UKHO Bathymetry Data Licence v1.0: commercial use allowed, "including it in your own product or application".
Attribution "Contains United Kingdom Hydrographic Office data © Crown copyright and database right". The product must
not be presented as sanctioned for navigation or as a substitute for official navigational products. The licence runs
12 months, then until either side gives 6 months' notice. Not scripted yet: the survey index is open, but every
download needs an ADMIRALTY account token; 7,391 separate surveys, about 553 GB, no merged grid.

**BSH NAUTHIS (Germany)** - its WFS download service is disabled (checked 2026-09-16).

**Kartverket ENC** - sold through PRIMAR, not open.
