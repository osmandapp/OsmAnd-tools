#ifndef APPLICATIONDATA_H
#define APPLICATIONDATA_H
#include <QObject>
#include <QStringList>

class ApplicationData : public QObject
{
    Q_OBJECT
public:
    explicit ApplicationData(QObject *parent = 0);
    Q_INVOKABLE QString setOsmandDirectiory(QString);
    QStringList files;
private:
    QString osmandDirectory;
signals:
    
public slots:

};
#endif // APPLICATIONDATA_H
