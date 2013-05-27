#ifndef MAPLAYERSDATA_H
#define MAPLAYERSDATA_H
#include "OsmAndApplication.h"
#include "Map.h"

class MapLayersData: public QObject
{
    Q_OBJECT
private:
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
public:
    explicit MapLayersData(QObject *parent = 0);

    Q_INVOKABLE int getMapZoom();
    Q_INVOKABLE double getMapLatitude();
    Q_INVOKABLE double getMapLongitude();
    Q_INVOKABLE void setMapLatLonZoom(double,double,int);
    Q_INVOKABLE bool isTargetPresent();
    Q_INVOKABLE double getTargetLatitude();
    Q_INVOKABLE double getTargetLongitude();
};

#endif // MAPLAYERSDATA_H
