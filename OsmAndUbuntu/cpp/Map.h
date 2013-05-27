#ifndef MAP_H
#define MAP_H

#include <QObject>
#include <QStringList>
#include <QRectF>
#include <Logging.h>
#include "OsmAndApplication.h"
#include "MapView.h"

class Map : public QObject
{
    Q_OBJECT
private:
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    std::shared_ptr<OsmAnd::OsmAndMapView> mapView;

public:
    explicit Map(QObject *parent = 0);

    Q_INVOKABLE void setBounds(int w, int h) {mapView->setBounds(w, h);}
    Q_INVOKABLE void setLatLon(float lat, float lon) {mapView->setLatLon(lat, lon); }
    Q_INVOKABLE void setZoom(int z) {mapView->setZoom(z);}
    Q_INVOKABLE int getZoom() {return mapView->getZoom();}
    Q_INVOKABLE void setRotate(float r) {mapView->setRotate(r);}
    Q_INVOKABLE int getRotate() {return mapView->getRotate();}
    Q_INVOKABLE QRectF getTiles() {return mapView->getTileRect(); }
    Q_INVOKABLE float getTileSize() {return mapView->getTileSize(); }
    Q_INVOKABLE int getCenterPointX() {return mapView->getCenterPointX(); }
    Q_INVOKABLE int getCenterPointY() {return mapView->getCenterPointY();}
    Q_INVOKABLE float getXTile() {return mapView->getXTile();}
    Q_INVOKABLE float getYTile() {return mapView->getYTile(); }
    Q_INVOKABLE float getLat() {return mapView->getLatitude();}
    Q_INVOKABLE float getLon() {return mapView->getLongitude(); }
    Q_INVOKABLE void moveTo(int dx, int dy) {mapView->moveTo(dx, dy);}
    Q_INVOKABLE int getRotatedMapXForPoint(double lat, double lon) {mapView->getRotatedMapXForPoint(lat, lon); }
    Q_INVOKABLE int getRotatedMapYForPoint(double lat, double lon) {mapView->getRotatedMapYForPoint(lat, lon); }

};

#endif // MAP_H
