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
METADATA - must be stored in GPX metadata extensions
SPECIAL - tag is used/generated/transformed in a special way
```

## 2. Save/Restore (OsmAnd GPX appearance tags)

### 2.1. Track

```
GPX=osmand:color OBF=color POI,MAP,STYLE,SPECIAL
GPX=osmand:width OBF=width POI,MAP,STYLE

GPX=osmand:activity OBF=route_activity_type POI,METADATA

GPX=osmand:points_groups OBF=points_groups_names POI
GPX=osmand:points_groups OBF=points_groups_icons POI
GPX=osmand:points_groups OBF=points_groups_colors POI
GPX=osmand:points_groups OBF=points_groups_backgrounds POI

GPX=osmand:coloring_type OBF=coloring_type POI
GPX=osmand:color_palette OBF=color_palette POI
GPX=osmand:elevation_meters OBF=elevation_meters POI
GPX=osmand:line_3d_visualization_by_type OBF=line_3d_visualization_by_type POI
GPX=osmand:line_3d_visualization_position_type OBF=line_3d_visualization_position_type POI
GPX=osmand:line_3d_visualization_wall_color_type OBF=line_3d_visualization_wall_color_type POI
GPX=osmand:show_arrows OBF=show_arrows POI
GPX=osmand:show_start_finish OBF=show_start_finish POI
GPX=osmand:split_interval OBF=split_interval POI
GPX=osmand:split_type OBF=split_type POI
GPX=osmand:vertical_exaggeration_scale OBF=vertical_exaggeration_scale POI
```

### 2.2. Waypoints

```
GPX=osmand:color OBF=color POI,MAP,STYLE,SPECIAL
GPX=osmand:icon OBF=icon POI,STYLE
GPX=osmand:background OBF=background POI,STYLE
GPX=<type> OBF=points_groups_category POI
```

## 3. Save/Restore (shields and OSM-related tags)

```
GPX=osmand:* OBF=[POI|MAP] POI,MAP,STYLE (all tags are saved and some are displayed)

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
GPX=osmand:width OBF=width POI,MAP,STYLE (supported: thin/medium/bold, roadstyle or 1-24 custom)

GPX=osmand:shield_bg OBF=shield_bg POI,MAP,STYLE
GPX=osmand:shield_fg OBF=shield_fg POI,MAP,STYLE
GPX=osmand:shield_fg_2 OBF=shield_fg_2 POI,MAP,STYLE
GPX=osmand:shield_text OBF=shield_text POI,MAP,STYLE
GPX=osmand:shield_textcolor OBF=shield_textcolor POI,MAP,STYLE

GPX=osmand:translucent_line_colors OBF=translucent_line_colors POI,MAP,STYLE (semi-transparent line colors)
```

### 5.2. Waypoints

```
GPX=<wpt> OBF=route VAL=point
GPX=osmand:icon OBF=icon POI,STYLE
GPX=osmand:color OBF=color POI,MAP,STYLE,SPECIAL
GPX=osmand:background OBF=background POI,STYLE (mix color+background is badly supported due to lack of icons)
```

## 6. Categories/Search (POI-section)

### 6.1. POI-categories (search, filters, show-on-map)

```
GPX=osmand:route_type OBF=route_type VALUE=[top level (group id) from activities.json] -- automatically resolved, "other" used as default
GPX=osmand:(route_activity_type|osmand:activity|route) OBF=route_activity_type VALUE=[sub level (activity id) from activities.json] -- resolved by tags

GPX=<wpt> OBF=route_track_point POI (Other routes points)
```

### 6.2. Search (by ref/name)

```
GPX=<name>|osmand:name|osmand:ref|osmand:shield_text OBF=GPX POI,SPECIAL
```

### 7. TODO

```
metadata_extra_tags, extensions_extra_tags, wpt_extra_tags (internal)
name, ref, description, filename
route_name, route_id (OSM/GPX)
TODO color, colour
```
