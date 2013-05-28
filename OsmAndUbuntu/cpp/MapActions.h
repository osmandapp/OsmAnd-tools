#ifndef MAPACTIONS_H
#define MAPACTIONS_H

#include <QObject>
#include <ObfReader.h>
#include <QThreadPool>
#include "MapLayersData.h"
#include "OsmAndApplication.h"


class MapActions : public QObject
{
    Q_OBJECT
private:
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    MapLayersData* data;
    QThreadPool threadPool;
    int pseudoCounter;

    void start(QRunnable *r) {pseudoCounter++; threadPool.start(r);}
    // This method is called to let UI know that there is know active threads
    void taskFinished() {pseudoCounter--;}


public:
    explicit MapActions(MapLayersData* d, QObject *parent = 0);

    Q_INVOKABLE void calculateRoute();
    Q_INVOKABLE bool isActivityRunning();
    
    friend class RunRouteCalculation;
};

#endif // MAPACTIONS_H
