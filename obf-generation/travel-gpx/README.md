# TravelGpx GPX<->OBF tags schema (2024/11/29)
### Generation: OsmGpxWriteContext, RouteRelationExtractor
### Application: TravelObfHelper, TravelGpx, resources, etc

## 1. Notes

```
OBF - located in OBF file
GPX - located in GPX file
POI - located in poi_types.xml
MAP - located in rendering_types.xml
STYLE - located in routes.addon.render.xml
FLEXIBLE - not hardcoded in Java (**prefix-based**)
METADATA - must be stored in GPX metadata extensions
SPECIAL - tag is used/generated/transformed in a special way
```

## 2. Save/Restore (OsmAnd GPX appearance tags)

### 2.1. Track

```
GPX=osmand:color OBF=color POI,MAP,STYLE,SPECIAL
GPX=osmand:width OBF=gpx_width POI,MAP,STYLE

GPX=osmand:activity OBF=gpx_osmand:activity POI,METADATA

GPX=osmand:points_groups OBF=points_groups_names POI
GPX=osmand:points_groups OBF=points_groups_icons POI
GPX=osmand:points_groups OBF=points_groups_colors POI
GPX=osmand:points_groups OBF=points_groups_backgrounds POI

GPX=osmand:coloring_type OBF=gpx_coloring_type POI,FLEXIBLE
GPX=osmand:color_palette OBF=gpx_color_palette POI,FLEXIBLE
GPX=osmand:elevation_meters OBF=gpx_elevation_meters POI,FLEXIBLE
GPX=osmand:line_3d_visualization_by_type OBF=gpx_line_3d_visualization_by_type POI,FLEXIBLE
GPX=osmand:line_3d_visualization_position_type OBF=gpx_line_3d_visualization_position_type POI,FLEXIBLE
GPX=osmand:line_3d_visualization_wall_color_type OBF=gpx_line_3d_visualization_wall_color_type POI,FLEXIBLE
GPX=osmand:show_arrows OBF=gpx_show_arrows POI,FLEXIBLE
GPX=osmand:show_start_finish OBF=gpx_show_start_finish POI,FLEXIBLE
GPX=osmand:split_interval OBF=gpx_split_interval POI,FLEXIBLE
GPX=osmand:split_type OBF=gpx_split_type POI,FLEXIBLE
GPX=osmand:vertical_exaggeration_scale OBF=gpx_vertical_exaggeration_scale POI,FLEXIBLE
```

### 2.2. Waypoints

```
GPX=osmand:color OBF=color POI,MAP,STYLE,SPECIAL
GPX=osmand:icon OBF=gpx_icon POI,STYLE
GPX=osmand:background OBF=gpx_bg POI,STYLE
GPX=<type> OBF=points_groups_category POI
```

## 3. Save/Restore (shields and OSM-related tags)

```
GPX=osmand:osm_tag_* OBF=[POI|MAP] POI,MAP,STYLE,FLEXIBLE (some OSM tags will be lost: osmc:symbol, route, type, etc)

GPX=osmand:flexible_line_width OBF=flexible_line_width POI,MAP,STYLE
GPX=osmand:translucent_line_colors OBF=translucent_line_colors POI,MAP,STYLE

GPX=osmand:shield_bg OBF=shield_bg POI,MAP,STYLE
GPX=osmand:shield_fg OBF=shield_fg POI,MAP,STYLE
GPX=osmand:shield_fg_2 OBF=shield_fg_2 POI,MAP,STYLE
GPX=osmand:shield_text OBF=shield_text POI,MAP,STYLE
GPX=osmand:shield_textcolor OBF=shield_textcolor POI,MAP,STYLE
GPX=osmand:shield_waycolor OBF=shield_waycolor POI,SPECIAL (shadow copy of osmc_waycolor)
GPX=<none> OBF=shield_stub_name MAP,STYLE,SPECIAL (allows display shields without any text inside)
```

## 4. Save/Restore (analytics, elevation and special tags)

```
OBF=route_id POI,MAP,SPECIAL (used to identify unique GPX and to connect Map<->POI data) format:[A-Z]+[0-9]+ eg OSM12345

OBF=route_radius POI (specify default radius to search in POI section)

OBF=distance POI (distance of all GPX segments)
OBF=max_speed,avg_speed,min_speed POI (speed analytics)
OBF=time_span,time_span_no_gaps,time_moving,time_moving_no_gaps POI (time analytics)
OBF=start_ele,ele_graph MAP (GPX-specified elevation data wrapped in compact binary array)
OBF=min_ele,avg_ele,max_ele,diff_ele_up,diff_ele_down POI,MAP (analytics of GPX-specified elevation data)
```

## 5. Rendering (Map-section)

### 5.1. Tracks

```
GPX=<trkseg> OBF=route VAL=segment

GPX=osmand:color OBF=color POI,MAP,STYLE,SPECIAL
GPX=osmand:width OBF=gpx_width POI,MAP,STYLE (fixed width values supported)

GPX=osmand:shield_bg OBF=shield_bg POI,MAP,STYLE
GPX=osmand:shield_fg OBF=shield_fg POI,MAP,STYLE
GPX=osmand:shield_fg_2 OBF=shield_fg_2 POI,MAP,STYLE
GPX=osmand:shield_text OBF=shield_text POI,MAP,STYLE
GPX=osmand:shield_textcolor OBF=shield_textcolor POI,MAP,STYLE

GPX=osmand:flexible_line_width OBF=flexible_line_width POI,MAP,STYLE (zoom-based line width)
GPX=osmand:translucent_line_colors OBF=translucent_line_colors POI,MAP,STYLE (semi-transparent line colors)
```

### 5.2. Waypoints

```
GPX=<wpt> OBF=route VAL=point
GPX=osmand:icon OBF=gpx_icon POI,STYLE
GPX=osmand:color OBF=color POI,MAP,STYLE,SPECIAL
GPX=osmand:background OBF=gpx_bg POI,STYLE (mix color+gpx_bg is badly supported due to lack of icons)
```

## 6. Categories/Search (POI-section)

### 6.1. POI-categories (search, filters, show-on-map)

```
GPX=osmand:osm_tag_route OBF=route_type VALUE=cycling POI (Bicycle)
GPX=osmand:osm_tag_route OBF=route_type VALUE=fitness POI (Fitness trails)
GPX=osmand:osm_tag_route OBF=route_type VALUE=hiking POI (Hiking)
GPX=osmand:osm_tag_route OBF=route_type VALUE=inline_skates POI (Inline skates)
GPX=osmand:osm_tag_route OBF=route_type VALUE=mountainbike POI (Mountain biking)
GPX=osmand:osm_tag_route OBF=route_type VALUE=riding POI (Horse riding)
GPX=osmand:osm_tag_route OBF=route_type VALUE=running POI (Running)
GPX=osmand:osm_tag_route OBF=route_type VALUE=snowmobile POI (Snowmobile)
GPX=osmand:osm_tag_route OBF=route_type VALUE=walking POI (Walking)
GPX=osmand:osm_tag_route OBF=route_type VALUE=water POI (Water sports)
GPX=osmand:osm_tag_route OBF=route_type VALUE=winter POI (Winter sports)

GPX=[default] OBF=route_type VALUE=track POI (Other routes)

GPX=<wpt> OBF=route_track_point POI (Other routes points)
```

### 6.2. Search (by ref/name)

```
GPX=<name>|osmand:osm_tag_name|osmand:osm_tag_ref|osmand:osm_tag_description OBF=name POI,SPECIAL
GPX=osmand:osm_tag_ref|synthetic-ref OBF=name:ref POI,SPECIAL
```

### 7. TODO

```
remove gpx_ and osm_tag_ prefixes
describe name, ref, description, filename, color, colour
```
