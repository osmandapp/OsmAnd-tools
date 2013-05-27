#ifndef ROOTCONTEXT_H
#define ROOTCONTEXT_H
#include <QObject>
#include <QQmlContext>
#include  "MainApplicationSettings.h"
#include "Map.h"
#include "MapLayersData.h"
#include "MapActions.h"

class RootContext : public QObject {
    Q_OBJECT
private :
    MainApplicationSettings appData;
    Map mapData;
    MapLayersData mapLayerData;
    MapActions mapActions;
public:
    explicit RootContext(QObject *parent = 0) :mapActions(&mapLayerData) {
    }
    virtual ~RootContext() {}

    void createProperties(QQmlContext* r) {
        r->setContextProperty("applicationData", &appData);
        r->setContextProperty("mapData", &mapData);
        r->setContextProperty("mapLayerData", &mapLayerData);
        r->setContextProperty("mapActions", &mapActions);

    }
};

#endif // ROOTCONTEXT_H
