#ifndef MAPLAYERSDATA_H
#define MAPLAYERSDATA_H
#include "OsmAndCore.h"
#include "OsmAndApplication.h"
#include "RouteSegment.h"
#include "Map.h"

class MapLayersData: public QObject
{
    Q_OBJECT
private:
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    QList< OsmAnd::PointF > route;

public:
    explicit MapLayersData(QObject *parent = 0);

    Q_INVOKABLE int getMapZoom();
    Q_INVOKABLE double getMapLatitude();
    Q_INVOKABLE double getMapLongitude();
    Q_INVOKABLE void setMapLatLonZoom(double,double,int);

    void setRoute(QList< std::shared_ptr<OsmAnd::RouteSegment> >& r);
    Q_INVOKABLE int getRoutePointLength() { return route.size();}
    Q_INVOKABLE float getRoutePointLat(int i) {return route[i].y; }
    Q_INVOKABLE float getRoutePointLon(int i) {return route[i].x; }


    Q_INVOKABLE bool isTargetPresent();
    Q_INVOKABLE double getTargetLatitude();
    Q_INVOKABLE double getTargetLongitude();
    Q_INVOKABLE void setTargetLatLon(double lat,double lon);

    Q_INVOKABLE bool isStartPresent();
    Q_INVOKABLE double getStartLatitude();
    Q_INVOKABLE double getStartLongitude();
    Q_INVOKABLE void setStartLatLon(double lat,double lon);
};

#endif // MAPLAYERSDATA_H
