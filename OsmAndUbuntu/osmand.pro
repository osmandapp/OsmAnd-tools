QT += qml quick
# If your application uses the Qt Mobility libraries, uncomment the following
# lines and add the respective components to the MOBILITY variable.
# CONFIG += mobility
# MOBILITY +=

#C++ source files
SOURCES +=  cpp/main.cpp\
            cpp/applicationdata.cpp

QMAKE_CXXFLAGS +=-std=c++11

#C++ header files
HEADERS  += cpp/applicationdata.h

#Path to the libraries...
INCLUDEPATH +=  $$PWD\
                $$PWD/../../core/include $$PWD/../../core/include/native $$PWD/../../core/protos
DEPENDPATH += $$PWD/../../../../usr/lib

unix:!macx:
LIBS += -L$$PWD/../../binaries/linux/i686/Debug/ -lOsmAndCore

#Path to "other files" in this case the QML-Files
OTHER_FILES += \
    qml/main.qml\
    qml/*.qml
