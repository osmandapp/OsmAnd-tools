#include "Map.h"

Map::Map(QObject *) : app(OsmAnd::OsmAndApplication::getAndInitializeApplication())
{

    mapView = std::shared_ptr<OsmAnd::MapView>(new OsmAnd::MapView(app));
}
void Map::setBounds(int w, int h) {
    mapView->setBounds(w, h);
}

void Map::setLatLon(float lat, float lon) {
    mapView->setLatLon(lat, lon);
}

void Map::setZoom(int z)  {
    mapView->setZoom(z );
}

QRectF Map::getTiles() {
    return mapView->getTileRect();
}

int Map::getZoom() {
    return mapView->getZoom();
}

float Map::getTileSize() {
    return mapView->getTileSize();
}

int Map::getCenterPointX() {
    return mapView->getCenterPointX();
}

int Map::getCenterPointY() {
    return mapView->getCenterPointY();
}

float Map::getXTile() {
    return mapView->getXTile();
}

float Map::getYTile() {
    return mapView->getYTile();
}

void Map::moveTo(int dx, int dy) {
    mapView->moveTo(dx, dy);
}
