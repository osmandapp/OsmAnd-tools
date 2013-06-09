#ifndef MAPVIEWLAYER_H
#define MAPVIEWLAYER_H

#include <QObject>
#include <QImage>
#include <QMap>
#include <QQuickImageProvider>
#include "MapViewAdapter.h"
#include <OsmAndRasterMapLayer.h>

class MapViewLayer : public QObject, public QQuickImageProvider
{
    Q_OBJECT

    MapViewAdapter* adapter;
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    std::shared_ptr<OsmAnd::OsmAndRasterMapLayer> rasterLayer;
    QMap<QString, std::pair<OsmAnd::MapPoint, OsmAnd::MapPoint> > ids;
public:
    explicit MapViewLayer(MapViewAdapter* adapter, QObject *parent = 0);
    virtual QImage requestImage(const QString &id, QSize *size, const QSize &requestedSize);

    Q_INVOKABLE int left(const QString& id) { return ids[id].first.pixel().x; }
    Q_INVOKABLE int top(const QString& id) { return ids[id].first.pixel().y; }
    Q_INVOKABLE int right(const QString& id) { return ids[id].second.pixel().x; }
    Q_INVOKABLE int bottom(const QString& id) { return ids[id].second.pixel().y; }
};

#endif // MAPVIEWLAYER_H
