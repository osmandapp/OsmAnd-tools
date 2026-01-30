User Instructions
------------------

# Global Parameters

Below is a listing of all global parameters. Each parameter is accompanied by its purpose, default value, type, and any dependencies.

| Parameter              | Type | Default           | Purpose & Dependencies                                                       |
|------------------------|------|-------------------|------------------------------------------------------------------------------|
| CLICKHOUSE_HOST        | str  | 'data.osmand.net' | Hostname for ClickHouse connections. Required alongside `CLICKHOUSE_PWD`.    |
| CLICKHOUSE_PORT        | int  | 9000              | Port for ClickHouse connections.                                             |
| CLICKHOUSE_PWD         | str  |                   | Password for ClickHouse. **Required**.                                       |
| DB_TIMEOUT             | int  | 30                | Max execution time (seconds) for ClickHouse queries.                         |
|                        |      |                   |                                                                              |
| MODEL                  | str  |                   | LLM model name/ID. **Required**.                                             |
| API_URL                | str  | ''                | Base URL for the LLM API.                                                    |
| API_KEY                | str  |                   | API key for accessing the LLM. **Required**.                                 |
|                        |      |                   |                                                                              |
| PHOTOS_PER_PLACE       | int  | 40                | Number of images to request/download per place.                              |
| PROCESS_PLACES         | int  | 500               | Maximum number of places to process per run.                                 |
| PLACES_PER_THREAD      | int  | 10000             | Batch size for paginated queries/inserts (ex CHUNK_SIZE image-downloader).   |
| MAX_PLACES_PER_QUAD    | int  | 500               | Upper bound on places fetched per quad (should be less then PROCESS_PLACES). |
| MAX_PHOTOS_PER_REQUEST | int  | 15                | Maximum number of images to include in a single LLM call.                    |
| QUAD                   | str  | '**'              | Template string for quad search patterns.                                    |
| MIN_ELO                | int  | 200               | Minimum ELO rating to include a place in queries.                            |
| SAVE_SCORE_ENV         | int  | 0                 | Environment: 0 - Production, 1 - Test.                                       |
| PARALLEL               | int  | 10                | Number of threads in `ThreadPoolExecutor`.                                   |
| SELECTED_PLACE_IDS     | str  | ''                | Comma-separated list of place IDs. Either this or `QUAD` must be set.        |
| SELECTED_MEDIA_IDS     | str  | ''                | Comma-separated list of media IDs to make image selection.                   |
| MAX_IMG_DIMENSION      | int  | 720               | Upper bound for width/height when resizing images before LLM upload.         |
| DOWNLOAD_IF_EXISTS     | bool | false             | If `true`, force re-download even when a cached file already exists.         |
| CACHE_DIR              | str  | './wiki'          | Root directory for on-disk image cache.                                      |
| PROXY_FILE_URL         | str  | ''                | Path to a proxy URL of file (one proxy per line) used by image-downloader.   |
| OFFSET_BATCH           | int  | 0                 | Starting page offset when downloading images in chunks.                      |
| ERROR_LIMIT_PERCENT    | int  | 50                | Abort the whole image-downloader if error/success ratio reached.             |
| QUAD_ALPHABET          | str  | see code          | Character set expanded when `*` wildcards are present in `QUAD`.             |
| STATUS_TIME_OUT        | int  | 60                | Number of seconds as interval to check status of undone tasks.               |
| MIN_ELO_SUBTYPE        | int  | 800               | Minimum ELO rating to include a place which are in POI sub types.            |
| POI_SUBTYPE            | str  |                   | Comma separated list of POI sub types (e.g. "city, town").                   |
| MONITORING_INTERVAL    | int  | 600               | Interval for monitoring image-downloader stats (seconds)                     |
| ASTRO_IMAGES_ONLY      | bool | false             | Download images for lat==lon==0 objects only (image-downloader)              |
| MAX_TRIES              | int  | 10                | Number of attempts of image-downloader (useful with proxies)                 |
| MAX_SLEEP              | int  | 60                | Maximum sleep between HTTP-429 errors (image-downloader)                     |


### Threading Model

The heavy-I/O workers are coordinated via a `BoundedThreadPoolExecutor`, which caps concurrency with a semaphore and **allow task submission in bounded manner
from the main thread without blocking entire working stream**. Separate thread is checking status of all undone tasks from the executor and trace it to the log
to destroy job hanging illusion.

# 1. How to use TopPlaces job

- Frontend URL: https://maptile.osmand.net:4000/
- Jenkins job URL: https://data.osmand.net:8080/view/Wiki/job/Wiki_TopPlacePhotos/
- Set required environment variables (e.g., `MODEL`, `MAX_PLACES_PER_QUAD`, `PARALLEL`, `PHOTOS_PER_PLACE`, `MIN_ELO`, `SAVE_SCORE_ENV`) are described above.
- Either `QUAD` or `SELECTED_PLACE_IDS` must be provided.
- QUAD is used to filter places based on 'shortlink' field from ELO_RATING table.

## To score new images only in mix mode for all places.

- set QUAD string for quad search patterns which should includes one or more '*' symbol only. QUAD string pattern is number of '*' symbol to represent prefix
  symbols which built from "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~" alfabet symbols. For example, QUAD="*" will go though A to ~.

## To "restore" photo to Safe photo setup SELECTED_MEDIA_IDS and its corresponding SELECTED_PLACE_IDS:

- set SELECTED_MEDIA_IDS to ensure certain media IDs are included for rescoring.
- set SELECTED_PLACE_IDS to ensure certain place IDs are included for rescoring.

## To "rescore" top photos by setup SELECTED_PLACE_IDS:

- set SELECTED_PLACE_IDS ensuring certain place IDs are included for rescoring.

## To adjusting score formula or changing prompt:

- To calculate total score in final table should be changed SQL in top_places_index_final_photos.sql file where there are hardcoded weights. Main logic is in
  SQL: "WITH CAST((s_value * 0.50 + s_technical * 0.20 + s_overview * 0.15 + s_reality * 0.15) * 1000, 'UInt32') as weighted_score". Setup scoring weights (0.5,
  0.2, 0.15, 0.15) to balance score's (value, technical, overview, reality) impact. Sum of weights should be equals 1.0.
- Setup SAVE_SCORE_ENV=1 to TEST and test prompt to be sure it comply to your requirements.
- Setup SAVE_SCORE_ENV=0 to PRODUCTION and rescore specific SELECTED_PLACE_IDS by using updated prompt.

# 2. How to use TopPlaces_Duplicates job

- Jenkins job URL: https://veles.osmand.net:8080/job/Wiki_TopPlacePhotos_Duplicates
- Set required environment variables (e.g., `PROCESS_PLACES`, `MAX_PLACES_PER_QUAD`, `PARALLEL`, `PHOTOS_PER_PLACE`, `MIN_ELO`) are described in global
  parameters's table above.
- To rescore some places or specific photos setup SELECTED_MEDIA_IDS and its corresponding SELECTED_PLACE_IDS (as described above)

# 3. How to build Final scoring

- Jenkins job URL: https://data.osmand.net:8080/view/Wiki/job/Wiki_TopPhotos_GenFinal/
- Be sure all required photos are scored by using TopPlaces job in SAVE_SCORE_ENV=0 (Production)
- Be sure all required duplications are processed by using TopPlaces_Duplicates job
- Run Wiki_TopPhotos_GenFinal job
