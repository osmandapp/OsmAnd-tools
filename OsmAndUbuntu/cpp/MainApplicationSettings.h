#ifndef APPLICATIONDATA_H
#define APPLICATIONDATA_H
#include <QObject>
#include <QStringList>
#include <OsmAndApplication.h>

class MainApplicationSettings : public QObject
{
    Q_OBJECT
private:
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
public:
    explicit MainApplicationSettings(QObject *parent = 0);
    void reloadFiles();
    Q_INVOKABLE void setOsmandDirectiory(QString);
    Q_INVOKABLE QString getOsmandDirectiory();
    Q_INVOKABLE QStringList getFiles() { return files; }
    Q_INVOKABLE QString describeFile(int index);
    QStringList files;

signals:
    
public slots:

};
#endif // APPLICATIONDATA_H
