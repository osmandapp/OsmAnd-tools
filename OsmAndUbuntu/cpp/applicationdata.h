#ifndef APPLICATIONDATA_H
#define APPLICATIONDATA_H
#include <QObject>
#include <QStringList>
#include "OsmAndApplication.h"

class ApplicationData : public QObject
{
    Q_OBJECT
public:
    explicit ApplicationData(QObject *parent = 0);
    Q_INVOKABLE void setOsmandDirectiory(QString);
    Q_INVOKABLE QString getOsmandDirectiory();
    Q_INVOKABLE QStringList getFiles() { return files; }
    QStringList files;

signals:
    
public slots:

};
#endif // APPLICATIONDATA_H
