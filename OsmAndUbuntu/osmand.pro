QT += qml quick
# If your application uses the Qt Mobility libraries, uncomment the following
# lines and add the respective components to the MOBILITY variable.
# CONFIG += mobility
# MOBILITY +=

#C++ source files
SOURCES +=  cpp/main.cpp \
    cpp/MainApplicationSettings.cpp \
    cpp/Map.cpp

QMAKE_CXXFLAGS +=-std=c++11

#C++ header files
HEADERS  += \
    cpp/MainApplicationSettings.h \
    cpp/Map.h

#Path to the libraries...
INCLUDEPATH +=  $$PWD/../../core/externals/protobuf/upstream.patched/src/ \
                $$PWD/../../core/include $$PWD/../../core/include/native \
                $$PWD/../../core/protos \
                $$PWD
DEPENDPATH += $$PWD/../../../../usr/lib \
              $$PWD/../../core/externals/protobuf/upstream.patched \
              $$PWD/../../core/include $$PWD/../../core/include/native \
              $$PWD/../../core/protos

unix:!macx:
LIBS += -L$$PWD/../../binaries/linux/i686/Debug/ -lOsmAndCore

#Path to "other files" in this case the QML-Files
OTHER_FILES += \
    qml/main.qml\
    qml/*.qml \
    qml/files.qml \
    qml/map.qml
