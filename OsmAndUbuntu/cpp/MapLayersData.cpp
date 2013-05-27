#include "MapLayersData.h"

MapLayersData::MapLayersData(QObject *) : app(OsmAnd::OsmAndApplication::getAndInitializeApplication())
{

}

void MapLayersData::setMapLatLonZoom(double lat,double lon,int zoom) {
    app->getSettings()->MAP_SHOW_LATITUDE.set(QVariant(lat));
    app->getSettings()->MAP_SHOW_LONGITUDE.set(QVariant(lon));
    app->getSettings()->MAP_SHOW_ZOOM.set(QVariant(zoom));
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
    return true;
    //return app->getSettings()->TARGET_LATITUDE.present();
}

double MapLayersData::getTargetLatitude() {
//    return app->getSettings()->TARGET_LATITUDE.get().toDouble();
    return 51;
}

double MapLayersData::getTargetLongitude() {
//    return app->getSettings()->TARGET_LONGITUDE.get().toDouble();
    return 4;
}
