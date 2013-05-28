QT += qml quick
# If your application uses the Qt Mobility libraries, uncomment the following
# lines and add the respective components to the MOBILITY variable.
# CONFIG += mobility
# MOBILITY +=

#C++ source files
SOURCES +=  cpp/main.cpp \
    cpp/MainApplicationSettings.cpp \
    cpp/MapLayersData.cpp \
    cpp/MapActions.cpp \
    cpp/MapViewAdapter.cpp \
    cpp/MapViewLayer.cpp

QMAKE_CXXFLAGS +=-std=c++11 -DSK_ALLOW_STATIC_GLOBAL_INITIALIZERS=0 \
        -DSK_RELEASE -DSK_CPU_LENDIAN

#C++ header files
HEADERS  += \
    cpp/MainApplicationSettings.h \
    cpp/MapLayersData.h \
    cpp/MapActions.h \
    cpp/RootContext.h \
    cpp/MapViewAdapter.h \
    cpp/MapViewLayer.h


SKIA_PATCHED = $$PWD/../../core/externals/skia/upstream.patched/

#Path to the libraries...
INCLUDEPATH +=  $$PWD/../../core/externals/protobuf/upstream.patched/src/ \
                $$PWD/../../core/client/ \
                $$PWD/../../core/include $$PWD/../../core/include/native \
                $$PWD/../../core/protos \
                $$SKIA_PATCHED/include/core  $$SKIA_PATCHED/include/utils \
                $$SKIA_PATCHED/include/config $$SKIA_PATCHED/include/effects \
                $$SKIA_PATCHED/include/src \
                $$PWD
DEPENDPATH += $$PWD/../../../../usr/lib \
              $$PWD/../../core/client/  \
              $$PWD/../../core/externals/protobuf/upstream.patched \
              $$PWD/../../core/include $$PWD/../../core/include/native \
              $$PWD/../../core/protos
#Path to "other files" in this case the QML-Files
OTHER_FILES += \
    qml/main.qml\
    qml/*.qml \
    qml/files.qml \
    qml/map.qml

unix:!macx:
LIBS += -L$$PWD/../../binaries/linux/i686/Debug/ -lOsmAndCore


