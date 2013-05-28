#ifndef MAPACTIONS_H
#define MAPACTIONS_H

#include <QObject>
#include <ObfReader.h>
#include <QThreadPool>
#include <OsmAndApplication.h>

#include "MapLayersData.h"



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


    friend class RunRouteCalculation;
    friend class RunRouteRasterization;
public:
    explicit MapActions(MapLayersData* d, QObject *parent = 0);

    Q_INVOKABLE void calculateRoute();
    void runRasterization(OsmAnd::AreaI bbox, uint32_t zoom);
    Q_INVOKABLE bool isActivityRunning();
    

};

#endif // MAPACTIONS_H
