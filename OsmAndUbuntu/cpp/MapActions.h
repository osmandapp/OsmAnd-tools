#ifndef MAPACTIONS_H
#define MAPACTIONS_H

#include <QObject>
#include <ObfReader.h>
#include "MapLayersData.h"
#include "OsmAndApplication.h"

class MapActions : public QObject
{
    Q_OBJECT
private:
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    MapLayersData* data;

public:
    explicit MapActions(MapLayersData* d, QObject *parent = 0);

    Q_INVOKABLE QString calculateRoute();
    
signals:
    
public slots:
    
};

#endif // MAPACTIONS_H
