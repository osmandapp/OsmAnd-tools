#include "MapLayersData.h"

MapLayersData::MapLayersData(QObject *) : app(OsmAnd::OsmAndApplication::getAndInitializeApplication())
{

}

void MapLayersData::setMapLatLonZoom(double lat,double lon,int zoom) {
    app->getSettings()->MAP_SHOW_LATITUDE.set(QVariant(lat));
    app->getSettings()->MAP_SHOW_LONGITUDE.set(QVariant(lon));
    app->getSettings()->MAP_SHOW_ZOOM.set(QVariant(zoom));
}

void MapLayersData::setTargetLatLon(double lat,double lon) {
    app->getSettings()->TARGET_LATITUDE.set(QVariant(lat));
    app->getSettings()->TARGET_LONGITUDE.set(QVariant(lon));
}

void MapLayersData::setStartLatLon(double lat,double lon) {
    app->getSettings()->START_LATITUDE.set(QVariant(lat));
    app->getSettings()->START_LONGITUDE.set(QVariant(lon));
}

double MapLayersData::getMapLatitude() {
    return app->getSettings()->MAP_SHOW_LATITUDE.get().toDouble();
}

double MapLayersData::getMapLongitude() {
    return app->getSettings()->MAP_SHOW_LONGITUDE.get().toDouble();
}

int MapLayersData::getMapZoom() {
    return app->getSettings()->MAP_SHOW_ZOOM.get().toInt();
}

bool MapLayersData::isTargetPresent() {
    return app->getSettings()->TARGET_LATITUDE.present();
}

double MapLayersData::getTargetLatitude() {
    return app->getSettings()->TARGET_LATITUDE.get().toDouble();
}

double MapLayersData::getTargetLongitude() {
    return app->getSettings()->TARGET_LONGITUDE.get().toDouble();
}

bool MapLayersData::isStartPresent() {
    return app->getSettings()->START_LATITUDE.present();
}

double MapLayersData::getStartLatitude() {
    return app->getSettings()->START_LATITUDE.get().toDouble();
}

double MapLayersData::getStartLongitude() {
    return app->getSettings()->START_LONGITUDE.get().toDouble();
}
