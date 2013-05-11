#ifndef APPLICATIONDATA_H
#define APPLICATIONDATA_H
#include <QObject>

class ApplicationData : public QObject
{
    Q_OBJECT
public:
    explicit ApplicationData(QObject *parent = 0);
    Q_INVOKABLE QString calculate(QString) const;
signals:
    
public slots:
};
#endif // APPLICATIONDATA_H
