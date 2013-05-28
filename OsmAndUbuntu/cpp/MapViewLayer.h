#ifndef MAPVIEWLAYER_H
#define MAPVIEWLAYER_H

#include <QObject>
#include <QImage>
#include <QMap>
#include <QQuickImageProvider>
#include "MapViewAdapter.h"

class MapViewLayer : public QObject, public QQuickImageProvider
{
    Q_OBJECT

    MapViewAdapter* adapter;
    QPixmap* img;
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    QMap<QString, QImage* > cache;

    const QImage* loadImage(QString& s);
public:
    explicit MapViewLayer(MapViewAdapter* adapter, QObject *parent = 0);
    virtual QPixmap requestPixmap(const QString &id, QSize *size, const QSize& requestedSize);
};

#endif // MAPVIEWLAYER_H
