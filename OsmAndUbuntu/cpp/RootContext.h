#ifndef ROOTCONTEXT_H
#define ROOTCONTEXT_H
#include <QObject>
#include <QQmlContext>
#include  "MainApplicationSettings.h"
#include "MapViewAdapter.h"
#include "MapLayersData.h"
#include "MapViewLayer.h"
#include "MapActions.h"

class RootContext : public QObject {
    Q_OBJECT
private :
    MainApplicationSettings appData;
    MapViewAdapter mapViewAdapter;
    MapLayersData mapLayerData;
    MapActions mapActions;
    MapViewLayer mapViewLayer;
public:
    explicit RootContext(QObject *parent = 0) :
        mapActions(&mapLayerData), mapViewLayer(&mapViewAdapter) {
    }
    virtual ~RootContext() {}

    MapViewAdapter* getMapViewAdapter() {return &mapViewAdapter;}
    MapLayersData* getMapLayersData() {return &mapLayerData;}
    MapViewLayer* getMapViewLayer() {return &mapViewLayer;}

    void createProperties(QQmlContext* r) {
        r->setContextProperty("applicationData", &appData);
        r->setContextProperty("mapViewAdapter", &mapViewAdapter);
        r->setContextProperty("mapViewLayer", &mapViewLayer);
        r->setContextProperty("mapLayerData", &mapLayerData);
        r->setContextProperty("mapActions", &mapActions);

    }
};

#endif // ROOTCONTEXT_H
