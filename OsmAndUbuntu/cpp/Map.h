#ifndef MAP_H
#define MAP_H

#include <QObject>
#include <QStringList>
#include <QRectF>
#include "OsmAndApplication.h"
#include "MapView.h"

class Map : public QObject
{
    Q_OBJECT
private:
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    std::shared_ptr<OsmAnd::MapView> mapView;

public:
    explicit Map(QObject *parent = 0);

    Q_INVOKABLE void setBounds(int, int);
    Q_INVOKABLE void setLatLon(float, float);
    Q_INVOKABLE void setZoom(int);
    Q_INVOKABLE int getZoom();
    Q_INVOKABLE QRectF getTiles();
    Q_INVOKABLE float getTileSize();
    Q_INVOKABLE int getCenterPointX();
    Q_INVOKABLE int getCenterPointY();
    Q_INVOKABLE float getXTile();
    Q_INVOKABLE float getYTile();
    Q_INVOKABLE void moveTo(int, int);

};

#endif // MAP_H
