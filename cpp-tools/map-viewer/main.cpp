#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <memory>
#include <chrono>
#include <future>
#include <fstream>
#include <locale.h>

#include <GL/glew.h>
#include <GL/freeglut.h>

#include <OsmAndCore/QtExtensions.h>
#include <QString>
#include <QList>
#include <QFile>
#include <QMutex>
#include <QElapsedTimer>
#include <QProcess>
#include <QStringBuilder>
#include <QTemporaryFile>
#include <QTextStream>

#include <OsmAndCore.h>
#include <OsmAndCore/Search/CoordinateSearch.h>
#include <OsmAndCore/Common.h>
#include <OsmAndCore/QuadTree.h>
#include <OsmAndCore/Utilities.h>
#include <OsmAndCore/Logging.h>
#include <OsmAndCore/Stopwatch.h>
#include <OsmAndCore/CoreResourcesEmbeddedBundle.h>
#include <OsmAndCore/ResourcesManager.h>
#include <OsmAndCore/ObfsCollection.h>
#include <OsmAndCore/ObfDataInterface.h>
#include <OsmAndCore/WorldRegion.h>
#include <OsmAndCore/WorldRegions.h>
#include <OsmAndCore/RoadLocator.h>
#include <OsmAndCore/IRoadLocator.h>
#include <OsmAndCore/TileSqliteDatabasesCollection.h>
#include <OsmAndCore/GeoTiffCollection.h>
#include <OsmAndCore/Data/Road.h>
#include <OsmAndCore/Data/ObfRoutingSectionInfo.h>
#include <OsmAndCore/Data/Amenity.h>
#include <OsmAndCore/Map/MapRasterizer.h>
#include <OsmAndCore/Map/MapPresentationEnvironment.h>
#include <OsmAndCore/Map/IMapStylesCollection.h>
#include <OsmAndCore/Map/MapStylesCollection.h>
#include <OsmAndCore/Map/MapStyleEvaluator.h>
#include <OsmAndCore/Map/IMapRenderer.h>
#include <OsmAndCore/Map/IMapRenderer_Metrics.h>
#include <OsmAndCore/Map/IAtlasMapRenderer.h>
#include <OsmAndCore/Map/AtlasMapRendererConfiguration.h>
#include <OsmAndCore/Map/AtlasMapRenderer_Metrics.h>
#include <OsmAndCore/Map/OnlineRasterMapLayerProvider.h>
#include <OsmAndCore/Map/OnlineTileSources.h>
#include <OsmAndCore/Map/IMapElevationDataProvider.h>
#include <OsmAndCore/Map/SqliteHeightmapTileProvider.h>
#include <OsmAndCore/Map/HillshadeRasterMapLayerProvider.h>
#include <OsmAndCore/Map/SlopeRasterMapLayerProvider.h>
#include <OsmAndCore/Map/ObfMapObjectsProvider.h>
#include <OsmAndCore/Map/MapPrimitivesProvider.h>
#include <OsmAndCore/Map/MapRasterLayerProvider_Software.h>
#include <OsmAndCore/Map/MapRasterLayerProvider_GPU.h>
#include <OsmAndCore/Map/MapRasterMetricsLayerProvider.h>
#include <OsmAndCore/Map/ObfMapObjectsMetricsLayerProvider.h>
#include <OsmAndCore/Map/MapPrimitivesProvider.h>
#include <OsmAndCore/Map/MapPrimitivesMetricsLayerProvider.h>
#include <OsmAndCore/Map/MapObjectsSymbolsProvider.h>
#include <OsmAndCore/Map/MapMarkersCollection.h>
#include <OsmAndCore/Map/MapMarkerBuilder.h>
#include <OsmAndCore/Map/MapMarker.h>
#include <OsmAndCore/Map/MapAnimator.h>
#include <OsmAndCore/Map/RasterMapSymbol.h>
#include <OsmAndCore/Map/IBillboardMapSymbol.h>
#include <OsmAndCore/Map/AmenitySymbolsProvider.h>
#include <OsmAndCore/FavoriteLocationsGpxCollection.h>
#include <OsmAndCore/Map/FavoriteLocationsPresenter.h>
#include <OsmAndCore/GpxDocument.h>
#include <OsmAndCore/Search/AmenitiesByNameSearch.h>
#include <OsmAndCore/Search/AmenitiesInAreaSearch.h>
#include <OsmAndCore/Search/AddressesByNameSearch.h>
#include <OsmAndCore/ValueAnimator.h>
#include <OsmAndCore/Search/ReverseGeocoder.h>

bool glutWasInitialized = false;
QMutex glutWasInitializedFlagMutex;

OsmAnd::AreaI viewport;
std::shared_ptr<OsmAnd::IMapRenderer> renderer;
std::shared_ptr<OsmAnd::ResourcesManager> resourcesManager;
std::shared_ptr<const OsmAnd::IObfsCollection> obfsCollection;
std::shared_ptr<const OsmAnd::ITileSqliteDatabasesCollection> heightsCollection;
std::shared_ptr<const OsmAnd::IGeoTiffCollection> geotiffCollection;
std::shared_ptr<OsmAnd::ObfMapObjectsProvider> binaryMapObjectsProvider;
std::shared_ptr<OsmAnd::MapPresentationEnvironment> mapPresentationEnvironment;
std::shared_ptr<OsmAnd::MapPrimitiviser> primitivizer;
std::shared_ptr<OsmAnd::MapPrimitivesProvider> mapPrimitivesProvider;
std::shared_ptr<const OsmAnd::IMapStylesCollection> stylesCollection;
std::shared_ptr<const OsmAnd::ResolvedMapStyle> style;
std::shared_ptr<OsmAnd::MapAnimator> animator;
std::shared_ptr<OsmAnd::MapObjectsSymbolsProvider> mapObjectsSymbolsProvider;
std::shared_ptr<OsmAnd::AmenitySymbolsProvider> amenitySymbolsProvider;
std::shared_ptr<OsmAnd::MapMarkersCollection> markers;
std::shared_ptr<OsmAnd::FavoriteLocationsGpxCollection> favorites;
std::shared_ptr<OsmAnd::FavoriteLocationsPresenter> favoritesPresenter;
std::shared_ptr<OsmAnd::MapMarker> lastClickedLocationMarker;
std::shared_ptr<OsmAnd::RoadLocator> roadLocator;
//const auto obfMapObjectsProviderMode = OsmAnd::ObfMapObjectsProvider::Mode::OnlyBinaryMapObjects;
const auto obfMapObjectsProviderMode = OsmAnd::ObfMapObjectsProvider::Mode::BinaryMapObjectsAndRoads;

bool obfsDirSpecified = false;
QDir obfsDir;
bool dataDirSpecified = false;
QDir dataDir;
QDir cacheDir(QDir::current());
QDir heightsDir;
bool heightsDirSpecified = false;
QDir geotiffDir;
bool geotiffDirSpecified = false;
QFileInfoList styleFiles;
QString styleName = "default";

bool heightmap = false;
bool hillshade = false;

#define USE_GREEN_TEXT_COLOR 1
#define SCREEN_WIDTH 1024
#define SCREEN_HEIGHT 760
//#define SCREEN_WIDTH 256
//#define SCREEN_HEIGHT 256
//#define SCREEN_WIDTH 350
//#define SCREEN_HEIGHT 760

#if defined(WIN32)
const bool useGpuWorker = true;
//const bool useGpuWorker = false;
#else
const bool useGpuWorker = false;
#endif
bool useSpecificOpenGL = false;
bool use43 = false;
bool constantRefresh = false;
bool nSight = false;
bool gDEBugger = false;
bool profiler = false;
const float density = 1.0f;
const float mapScale = 1.0f;
const float symbolsScale = 1.0f;

bool renderWireframe = false;
void reshapeHandler(int newWidth, int newHeight);
void mouseHandler(int button, int state, int x, int y);
void mouseMotion(int x, int y);
void mouseWheelHandler(int button, int dir, int x, int y);
void keyboardHandler(unsigned char key, int x, int y);
void specialHandler(int key, int x, int y);
void displayHandler(void);
void idleHandler(void);
void closeHandler(void);
void activateProvider(int layerIdx, int idx);
void verifyOpenGL();

#if defined(OSMAND_TARGET_OS_macosx)
void x11Init();
void x11Release();
void x11AlterModifiers(int& modifiers);
#endif // defined(OSMAND_TARGET_OS_macosx)

int elevationConfigurationPresetIndex = 0;
std::pair<QString, OsmAnd::ElevationConfiguration> elevationConfigurationPresets[] =
{
    {
        QStringLiteral("none"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::None)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::None)
    },
    {
        QStringLiteral("hs-traditional-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeTraditional)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("hs-traditional-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeTraditional)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("hs-igor-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeIgor)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("hs-igor-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeIgor)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("hs-combined-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeCombined)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("hs-combined-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeCombined)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("hs-multidir-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeMultidirectional)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("hs-multidir-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::HillshadeMultidirectional)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleHillshade)
    },
    {
        QStringLiteral("slope°-grayscale-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopeDegrees)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleSlopeDegrees)
    },
    {
        QStringLiteral("slope°-grayscale-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopeDegrees)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleSlopeDegrees)
    },
    {
        QStringLiteral("slope°-terrain-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopeDegrees)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::TerrainSlopeDegrees)
    },
    {
        QStringLiteral("slope°-terrain-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopeDegrees)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::TerrainSlopeDegrees)
    },
    {
        QStringLiteral("slope%-grayscale-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopePercents)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleSlopeDegrees)
    },
    {
        QStringLiteral("slope%-grayscale-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopePercents)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::GrayscaleSlopeDegrees)
    },
    {
        QStringLiteral("slope%-terrain-ZT"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::ZevenbergenThorne)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopePercents)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::TerrainSlopeDegrees)
    },
    {
        QStringLiteral("slope%-terrain-H"),
        OsmAnd::ElevationConfiguration()
            .setSlopeAlgorithm(OsmAnd::ElevationConfiguration::SlopeAlgorithm::Horn)
            .setVisualizationStyle(OsmAnd::ElevationConfiguration::VisualizationStyle::SlopePercents)
            .setVisualizationColorMapPreset(OsmAnd::ElevationConfiguration::ColorMapPreset::TerrainSlopeDegrees)
    }
};
const int elevationConfigurationPresetsCount = std::size(elevationConfigurationPresets);

OsmAnd::PointI lastClickedLocation31;

int main(int argc, char** argv)
{
    //////////////////////////////////////////////////////////////////////////
    std::shared_ptr<const OsmAnd::CoreResourcesEmbeddedBundle> coreResourcesEmbeddedBundle;
#if defined(OSMAND_CORE_STATIC)
    std::cout << "Loading symbols from this executable" << std::endl;
    coreResourcesEmbeddedBundle = OsmAnd::CoreResourcesEmbeddedBundle::loadFromCurrentExecutable();
#else
    std::cout << "Loading symbols from 'OsmAndCore_ResourcesBundle_shared'" << std::endl;
    coreResourcesEmbeddedBundle = OsmAnd::CoreResourcesEmbeddedBundle::loadFromSharedResourcesBundle();
#endif // defined(OSMAND_CORE_STATIC)
    if (!OsmAnd::InitializeCore(coreResourcesEmbeddedBundle))
    {
        std::cerr << "Failed to initialize core" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }
    std::cout << "Initialized Core" << std::endl;

    //OsmAnd::Logger::get()->setSeverityLevelThreshold(OsmAnd::LogSeverityLevel::Error);

    //////////////////////////////////////////////////////////////////////////
    OsmAnd::ValueAnimator valueAnimator;
    valueAnimator.animateValueTo<float>(
        1.0f,
        0.0f,
        OsmAnd::ValueAnimator::TimingFunction::EaseInOutQuadratic,
        1.0f,
        nullptr,
        nullptr);
    //////////////////////////////////////////////////////////////////////////

    for (int argIdx = 1; argIdx < argc; argIdx++)
    {
        std::cout << "Arg: " << argv[argIdx] << std::endl;

        const QString arg(argv[argIdx]);

        if (arg.startsWith("-stylesPath="))
        {
            auto path = arg.mid(strlen("-stylesPath="));
            QDir dir(path);
            if (!dir.exists())
            {
                std::cerr << "Style directory '" << path.toStdString() << "' does not exist" << std::endl;
                OsmAnd::ReleaseCore();
                return EXIT_FAILURE;
            }

            OsmAnd::Utilities::findFiles(dir, QStringList() << "*.render.xml", styleFiles);
        }
        else if (arg.startsWith("-style="))
        {
            styleName = arg.mid(strlen("-style="));
        }
        else if (arg.startsWith("-obfsDir="))
        {
            auto obfsDirPath = arg.mid(strlen("-obfsDir="));
            obfsDir = QDir(obfsDirPath);
            obfsDirSpecified = true;
        }
        else if (arg.startsWith("-dataDir="))
        {
            auto dataDirPath = arg.mid(strlen("-dataDir="));
            dataDir = QDir(dataDirPath);
            dataDirSpecified = true;
        }
        else if (arg.startsWith("-cacheDir="))
        {
            cacheDir = QDir(arg.mid(strlen("-cacheDir=")));
        }
        else if (arg.startsWith("-heightsDir="))
        {
            heightsDir = QDir(arg.mid(strlen("-heightsDir=")));
            heightsDirSpecified = true;
        }
        else if (arg.startsWith("-geotiffDir="))
        {
            geotiffDir = QDir(arg.mid(strlen("-geotiffDir=")));
            geotiffDirSpecified = true;
        }
        else if (arg == "-nsight")
        {
            useSpecificOpenGL = true;
            use43 = true;
            constantRefresh = true;
            nSight = true;
            gDEBugger = false;
            profiler = false;
        }
        else if (arg == "-gdebugger")
        {
            useSpecificOpenGL = true;
            use43 = false;
            constantRefresh = true;
            nSight = false;
            gDEBugger = true;
            profiler = false;
        }
        else if (arg == "-profiler")
        {
            useSpecificOpenGL = true;
            use43 = false;
            constantRefresh = true;
            nSight = false;
            gDEBugger = false;
            profiler = true;
        }
    }

    if (!obfsDirSpecified && !dataDirSpecified)
    {
        std::cerr << "Nor OBFs directory nor data directory was specified (param -obfsDir=) " << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }
    if (obfsDirSpecified && !obfsDir.exists())
    {
        std::cerr << "OBFs directory does not exist" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }

    //////////////////////////////////////////////////////////////////////////

    {
        QMutexLocker scopedLocker(&glutWasInitializedFlagMutex);

        assert(glutInit != nullptr);
        glutInit(&argc, argv);

        glutSetOption(GLUT_ACTION_ON_WINDOW_CLOSE, GLUT_ACTION_CONTINUE_EXECUTION);
        glutInitWindowSize(SCREEN_WIDTH, SCREEN_HEIGHT);
        glutInitDisplayMode(GLUT_DOUBLE | GLUT_RGBA | GLUT_DEPTH);
        if (useSpecificOpenGL)
        {
            if (!use43)
                glutInitContextVersion(3, 0);
            else
                glutInitContextVersion(4, 3);
            //glutInitContextFlags(GLUT_DEBUG);
            glutInitContextProfile(GLUT_CORE_PROFILE);
        }
        assert(glutCreateWindow != nullptr);
        glutCreateWindow("OsmAnd Bird : 3D map render tool");

        glutReshapeFunc(&reshapeHandler);
        glutMouseFunc(&mouseHandler);
        glutMotionFunc(&mouseMotion);
        glutMouseWheelFunc(&mouseWheelHandler);
        glutKeyboardFunc(&keyboardHandler);
        glutSpecialFunc(&specialHandler);
        glutDisplayFunc(&displayHandler);
        glutIdleFunc(&idleHandler);
        glutCloseFunc(&closeHandler);
        verifyOpenGL();

#if defined(OSMAND_TARGET_OS_macosx)
        x11Init();
#endif // defined(OSMAND_TARGET_OS_macosx)

        glutWasInitialized = true;
    }

    //////////////////////////////////////////////////////////////////////////

    renderer = OsmAnd::createMapRenderer(OsmAnd::MapRendererClass::AtlasMapRenderer_OpenGL2plus);
    if (!renderer)
    {
        std::cout << "No supported renderer" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }
    animator = std::make_shared<OsmAnd::MapAnimator>();
    animator->setMapRenderer(renderer);
    //////////////////////////////////////////////////////////////////////////

    if (dataDirSpecified)
    {
        resourcesManager = std::make_shared<OsmAnd::ResourcesManager>(
            dataDir.absoluteFilePath(QLatin1String("storage")),
            dataDir.absoluteFilePath(QLatin1String("storage_ext")),
            QList<QString>(),
            dataDir.absoluteFilePath(QLatin1String("World_basemap_mini_2.obf")),
            dataDir.absoluteFilePath(QLatin1String("tmp")));

        const auto renderer_ = renderer;
        resourcesManager->localResourcesChangeObservable.attach(0,
            [renderer_]
        (const OsmAnd::ResourcesManager* const resourcesManager,
            const QList<QString>& added,
            const QList<QString>& removed,
            const QList<QString>& updated)
        {
            renderer_->reloadEverything();
        });

        obfsCollection = resourcesManager->obfsCollection;
        stylesCollection = resourcesManager->mapStylesCollection;
    }
    else if (obfsDirSpecified)
    {
        const auto manualObfsCollection = new OsmAnd::ObfsCollection();
        manualObfsCollection->addDirectory(obfsDir);

        obfsCollection.reset(manualObfsCollection);

        const auto pMapStylesCollection = new OsmAnd::MapStylesCollection();
        for (auto itStyleFile = styleFiles.begin(); itStyleFile != styleFiles.end(); ++itStyleFile)
        {
            const auto& styleFile = *itStyleFile;

            if (!pMapStylesCollection->addStyleFromFile(styleFile.absoluteFilePath()))
                std::cout << "Failed to parse metadata of '" << styleFile.fileName().toStdString() << "' or duplicate style" << std::endl;
        }
        stylesCollection.reset(pMapStylesCollection);
    }

    if (!styleName.isEmpty())
    {
        style = stylesCollection->getResolvedStyleByName(styleName);
        if (!style)
        {
            std::cout << "Failed to resolve style '" << styleName.toStdString() << "'" << std::endl;
            OsmAnd::ReleaseCore();
            return EXIT_FAILURE;
        }
    }

    roadLocator.reset(new OsmAnd::RoadLocator(obfsCollection));
    mapPresentationEnvironment.reset(new OsmAnd::MapPresentationEnvironment(
        style,
        density,
        mapScale,
        symbolsScale));
    mapPresentationEnvironment->setLocaleLanguageId("ru");
    mapPresentationEnvironment->setLanguagePreference(OsmAnd::MapPresentationEnvironment::LanguagePreference::LocalizedOrTransliterated);
    primitivizer.reset(new OsmAnd::MapPrimitiviser(mapPresentationEnvironment));

    OsmAnd::MapRendererSetupOptions rendererSetup;
    rendererSetup.frameUpdateRequestCallback =
        []
        (const OsmAnd::IMapRenderer* const mapRenderer)
        {
            //QMutexLocker scopedLocker(&glutWasInitializedFlagMutex);
            // sleep(1);
            if (glutWasInitialized) {
                glutPostRedisplay();
            }
        };
    rendererSetup.gpuWorkerThreadEnabled = useGpuWorker;
    if (rendererSetup.gpuWorkerThreadEnabled)
    {
#if defined(WIN32)
        const auto currentDC = wglGetCurrentDC();
        const auto currentContext = wglGetCurrentContext();
        const auto workerContext = wglCreateContext(currentDC);

        rendererSetup.gpuWorkerThreadEnabled = (wglShareLists(currentContext, workerContext) == TRUE);
        assert(currentContext == wglGetCurrentContext());

        rendererSetup.gpuWorkerThreadPrologue =
            [currentDC, workerContext]
        (const OsmAnd::IMapRenderer* const mapRenderer)
        {
            const auto result = (wglMakeCurrent(currentDC, workerContext) == TRUE);
            verifyOpenGL();
        };

        rendererSetup.gpuWorkerThreadEpilogue =
            []
        (const OsmAnd::IMapRenderer* const mapRenderer)
        {
            glFinish();
        };
#endif
    }
    renderer->setup(rendererSetup);

    const auto debugSettings = renderer->getDebugSettings();
    debugSettings->debugStageEnabled = true;
    renderer->setDebugSettings(debugSettings);

    viewport.top() = 0;
    viewport.left() = 0;
    viewport.bottom() = SCREEN_HEIGHT;
    viewport.right() = SCREEN_WIDTH;
//    viewport.top() = static_cast<int>(+0.0f * static_cast<float>(SCREEN_HEIGHT));
//    viewport.left() = static_cast<int>(+0.0f * static_cast<float>(SCREEN_WIDTH));
//    viewport.bottom() = static_cast<int>(+1.5f * static_cast<float>(SCREEN_HEIGHT));
//    viewport.right() = static_cast<int>(+1.0f * static_cast<float>(SCREEN_WIDTH));
    renderer->setWindowSize(OsmAnd::PointI(SCREEN_WIDTH, SCREEN_HEIGHT));
    renderer->setViewport(viewport);
    /*renderer->setTarget(OsmAnd::PointI(
        OsmAnd::Utilities::get31TileNumberX(34.0062),
        OsmAnd::Utilities::get31TileNumberY(44.4039)
        ));
        renderer->setZoom(18.0f);*/
    renderer->setZoom(1.5f);
    //renderer->setAzimuth(137.6f);
    renderer->setAzimuth(69.4f);
    //renderer->setElevationAngle(35.0f);
    renderer->setElevationAngle(90.0f);

    // Amsterdam
//    renderer->setTarget(OsmAnd::PointI(
//        1100178688,
//        704925203));
//    renderer->setZoom(12.0f);

    //renderer->setZoom(10.0f);
    //renderer->setZoom(4.0f);

//    renderer->setTarget(OsmAnd::Utilities::convertLatLonTo31(OsmAnd::LatLon(
//41.45811, 41.4537
//        )));
//    renderer->setZoom(12.0f);

//    // Kiev
//    renderer->setTarget(OsmAnd::PointI(
//        1255337783,
//        724166131));
//    ////renderer->setZoom(11.0f);
//    renderer->setZoom(8.0f);
//
//    // Synthetic
//    renderer->setTarget(OsmAnd::Utilities::convertLatLonTo31(OsmAnd::LatLon(
//        45.731606,
//        36.528217)));
//    renderer->setZoom(8.0f);

    // Nice
    renderer->setMapTarget(OsmAnd::PointI(SCREEN_WIDTH / 2, SCREEN_HEIGHT / 2),
        OsmAnd::Utilities::convertLatLonTo31(OsmAnd::LatLon(
            43.5804,
            7.1251)));
    renderer->setZoom(14.0f);

    renderer->setAzimuth(0.0f);

    // Elevation
    renderer->setElevationAngle(20.0f);
    renderer->setAzimuth(-30.0f);

    auto renderConfig = renderer->getConfiguration();
    renderer->setConfiguration(renderConfig);

    if (lastClickedLocationMarker)
        lastClickedLocationMarker->setPosition(renderer->getState().target31);

    bool ok = renderer->initializeRendering(true);
    assert(ok);
    //////////////////////////////////////////////////////////////////////////

    glutMainLoop();

    {
        QMutexLocker scopedLocker(&glutWasInitializedFlagMutex);

        glutWasInitialized = false;
    }

    renderer.reset();
    resourcesManager.reset();
    obfsCollection.reset();
    binaryMapObjectsProvider.reset();
    mapPrimitivesProvider.reset();
    stylesCollection.reset();
    style.reset();
    animator.reset();

    OsmAnd::ReleaseCore();
    return EXIT_SUCCESS;
}

void reshapeHandler(int newWidth, int newHeight)
{
    viewport.right() = newWidth;
    viewport.bottom() = newHeight;
    renderer->setWindowSize(OsmAnd::PointI(newWidth, newHeight));
    renderer->setViewport(viewport);

    glViewport(0, 0, newWidth, newHeight);
}

bool dragInitialized = false;
int dragInitX;
int dragInitY;
OsmAnd::PointI dragInitTarget;


std::unique_ptr<QProcess> zenity(const QString& cmd)
{
    auto p = std::unique_ptr<QProcess>(new QProcess);
    p->start(cmd);
    p->waitForFinished();
    return p;
}

QString inputDialog(const QString& title, const QString& text, const QString& entryText = "")
{
    // Cannot use QtGui unfortuanately (why?)
    // QString text = QInputDialog::getText(this, tr("Input coordinate"), tr("Coordinate:"), QLineEdit::rmal, "", &ok);
    // But since this program runs only on Linux... We will do some weird stuff.
    auto p = zenity("zenity --entry --title=\"" % title % "\" --text=\"" % text % "\" --entry-text \"" % entryText);
    return p->readAllStandardOutput().simplified();
}

void textInfoDialog(const QString& title, const QString& text)
{
    QTemporaryFile f;
    if (f.open()) {
        QTextStream stream(&f);
        stream << text;
        stream.flush();
        f.seek(0);
        zenity("zenity --text-info --title=\"" % title % "\" --filename=\"" % f.fileName() % "\"");
    }
}

void mouseHandler(int button, int state, int x, int y)
{
    auto modifiers = glutGetModifiers();
#if defined(OSMAND_TARGET_OS_macosx)
    x11AlterModifiers(modifiers);
#endif // defined(OSMAND_TARGET_OS_macosx)

    if (button == GLUT_LEFT_BUTTON)
    {
        if (state == GLUT_DOWN && !dragInitialized)
        {
            dragInitX = x;
            dragInitY = y;
            dragInitTarget = renderer->getState().target31;

            dragInitialized = true;
        }
        else if (state == GLUT_UP && dragInitialized)
        {
            dragInitialized = false;
        }
    }
    else if (button == GLUT_RIGHT_BUTTON)
    {
        if (state == GLUT_DOWN)
        {
            OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "--------------- click (%d, %d) -------------------", x, y);

            renderer->getLocationFromScreenPoint(OsmAnd::PointI(x, y), lastClickedLocation31);
            const auto delta = lastClickedLocation31 - renderer->getState().target31;
            OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "@ %d %d (offset from target %d %d)", lastClickedLocation31.x, lastClickedLocation31.y, delta.x, delta.y);


            if (modifiers & GLUT_ACTIVE_CTRL)
            {
                animator->pause();
                animator->cancelAllAnimations();
                //animator->animateTargetTo(lastClickedLocation31, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
                animator->parabolicAnimateTargetTo(lastClickedLocation31, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic, OsmAnd::MapAnimator::TimingFunction::EaseOutInQuadratic);
                animator->resume();
            }
            else if (modifiers & GLUT_ACTIVE_SHIFT)
            {
                if (favorites)
                    favorites->createFavoriteLocation(OsmAnd::Utilities::convert31ToLatLon(lastClickedLocation31));
            }
            else if (modifiers & GLUT_ACTIVE_ALT)
            {
                if (lastClickedLocationMarker)
                    lastClickedLocationMarker->setPosition(lastClickedLocation31);
            }

            // Road:
            const auto road = roadLocator->findNearestRoad(lastClickedLocation31, 500.0, OsmAnd::RoutingDataLevel::Detailed);
            if (road)
            {
                if (road->captions.isEmpty())
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "Found unnamed road");
                else
                {
                    const auto name = road->getCaptionInNativeLanguage();
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "Found road: %s", qPrintable(name));
                }
            }
            else
            {
                OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "No road found!");
            }

            // Map symbol
            const auto mapSymbolInfos = renderer->getSymbolsAt(OsmAnd::PointI(x, y));
            if (mapSymbolInfos.isEmpty())
                OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "No symbols found!");
            for (const auto& mapSymbolInfo : constOf(mapSymbolInfos))
            {
                const auto& mapSymbol = mapSymbolInfo.mapSymbol;

                OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "Clicked on map symbol %p", mapSymbol.get());
                if (const auto rasterMapSymbol = std::dynamic_pointer_cast<const OsmAnd::RasterMapSymbol>(mapSymbol))
                {
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - content = %s", qPrintable(rasterMapSymbol->content));
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - size = %d %d", rasterMapSymbol->size.x, rasterMapSymbol->size.y);
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - minDistance = %d", rasterMapSymbol->minDistance);
                }
                if (const auto billboardMapSymbol = std::dynamic_pointer_cast<const OsmAnd::IBillboardMapSymbol>(mapSymbol))
                {
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - offset = %d %d", billboardMapSymbol->getOffset().x, billboardMapSymbol->getOffset().y);
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - position31 = %d %d", billboardMapSymbol->getPosition31().x, billboardMapSymbol->getPosition31().y);
                }
                if (const auto group = mapSymbol->group.lock())
                {
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - symbols in group %d", group->symbols.size());
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - presentation mode %u", static_cast<unsigned int>(group->presentationMode));
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - from %s", qPrintable(group->toString()));

                    if (const auto mapObjectGroup = std::dynamic_pointer_cast<const OsmAnd::MapObjectsSymbolsProvider::MapObjectSymbolsGroup>(group))
                    {
                        if (const auto obfMapObject = std::dynamic_pointer_cast<const OsmAnd::ObfMapObject>(mapObjectGroup->mapObject))
                        {
                            std::shared_ptr<const OsmAnd::Amenity> amenity;
                            obfsCollection->obtainDataInterface()->findAmenityForObfMapObject(obfMapObject, &amenity);
                            if (amenity)
                            {
                                OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - has amenity");
                            }
                        }
                    }
                }
            }

            if (!constantRefresh)
                glutPostRedisplay();
        }
    }
}

void mouseMotion(int x, int y)
{
    if (dragInitialized)
    {
        auto deltaX = x - dragInitX;
        auto deltaY = y - dragInitY;

        const auto state = renderer->getState();

        // Azimuth
        auto angle = qDegreesToRadians(state.azimuth);
        auto cosAngle = cosf(angle);
        auto sinAngle = sinf(angle);

        auto nx = deltaX * cosAngle - deltaY * sinAngle;
        auto ny = deltaX * sinAngle + deltaY * cosAngle;

        const auto tileSize31 = (1u << (31 - state.zoomLevel));
        auto scale31 = static_cast<double>(tileSize31) /
            renderer->getTileSizeOnScreenInPixels();

        OsmAnd::PointI newTarget;
        newTarget.x = dragInitTarget.x - static_cast<int32_t>(nx * scale31);
        newTarget.y = dragInitTarget.y - static_cast<int32_t>(ny * scale31);

        renderer->setMapTargetLocation(newTarget);
    }
}

void mouseWheelHandler(int button, int dir, int x, int y)
{
    auto modifiers = glutGetModifiers();
#if defined(OSMAND_TARGET_OS_macosx)
    x11AlterModifiers(modifiers);
#endif // defined(OSMAND_TARGET_OS_macosx)

    if (modifiers & GLUT_ACTIVE_ALT)
    {
        const auto step = (modifiers & GLUT_ACTIVE_SHIFT) ? 1.0f : 0.1f;
        const auto state = renderer->getState();

        if (dir > 0)
        {
            renderer->setVisualZoomShift(state.visualZoomShift + step);
        }
        else
        {
            renderer->setVisualZoomShift(state.visualZoomShift - step);
        }
    }
    else
    {
        const auto step = (modifiers & GLUT_ACTIVE_SHIFT) ? 0.1f : 0.01f;
        const auto state = renderer->getState();

        const auto zoom = state.surfaceZoomLevel + (state.surfaceVisualZoom >= 1.0f ? state.surfaceVisualZoom - 1.0f : (state.surfaceVisualZoom - 1.0f) * 2.0f);
        if (dir > 0)
        {
            renderer->setZoom(zoom + step);
        }
        else
        {
            renderer->setZoom(zoom - step);
        }
    }
}

void keyboardHandler(unsigned char key, int x, int y)
{
    auto modifiers = glutGetModifiers();
#if defined(OSMAND_TARGET_OS_macosx)
    x11AlterModifiers(modifiers);
#endif // defined(OSMAND_TARGET_OS_macosx)

    const auto state = renderer->getState();
    const auto wasdZoom = static_cast<int>(
        state.surfaceZoomLevel + (state.surfaceVisualZoom >= 1.0f
                               ? state.surfaceVisualZoom - 1.0f
                               : (state.surfaceVisualZoom - 1.0f) * 2.0f));
    const auto wasdStep = (1 << (31 - wasdZoom));

    // ' ' * + - .  0 1 2 3 4 5 6 7 8 9 A D S W [ \\ \x1B ] a b c d e f g h i j k l m n o p q r s t u v w x z
    switch (key)
    {
    case '\x1B':
        glutLeaveMainLoop();
        return;
    case 'W':
    case 'w':
    {
        auto newTarget = state.target31;
        newTarget.y -= wasdStep / (key == 'w' ? 50 : 10);
        renderer->setMapTargetLocation(newTarget);
        return;
    }
    case 'S':
    case 's':
    {
        auto newTarget = state.target31;
        newTarget.y += wasdStep / (key == 's' ? 50 : 10);
        renderer->setMapTargetLocation(newTarget);
        return;
    }
    case 'A':
    case 'a':
    {
        auto newTarget = state.target31;
        newTarget.x -= wasdStep / (key == 'a' ? 50 : 10);
        renderer->setMapTargetLocation(newTarget);
        return;
    }
    case 'D':
    case 'd':
    {
        auto newTarget = state.target31;
        newTarget.x += wasdStep / (key == 'd' ? 50 : 10);
        renderer->setMapTargetLocation(newTarget);
        return;
    }
    case 'R':
    case 'r':
    {
        renderer->setVisibleDistance(state.visibleDistance + (key == 'R' ? 10.0f : 1.0f));
        return;
    }
    case 'F':
    case 'f':
    {
        renderer->setVisibleDistance(state.visibleDistance - (key == 'F' ? 10.0f : 1.0f));
        return;
    }
    case 'x':
        renderWireframe = !renderWireframe;
        glutPostRedisplay();
        return;
    case 'e':
        if (modifiers & GLUT_ACTIVE_ALT)
        {
            if (state.elevationDataProvider)
            {
                heightsCollection.reset();
                if (!hillshade)
                    geotiffCollection.reset();
                renderer->resetElevationDataProvider();
                heightmap = false;
            }
            else
            {
                if (heightsDirSpecified)
                {
                    const auto manualHeightsCollection = new OsmAnd::TileSqliteDatabasesCollection();
                    manualHeightsCollection->addDirectory(heightsDir);

                    heightsCollection.reset(manualHeightsCollection);
                }

                if (geotiffDirSpecified && !geotiffCollection)
                {
                    const auto manualGeoTiffCollection = new OsmAnd::GeoTiffCollection();
                    manualGeoTiffCollection->addDirectory(geotiffDir);
                    manualGeoTiffCollection->setLocalCache(cacheDir);

                    geotiffCollection.reset(manualGeoTiffCollection);
                }

                if (heightsCollection && geotiffCollection)
                {
                    renderer->setElevationDataProvider(
                        std::make_shared<OsmAnd::SqliteHeightmapTileProvider>(
                            heightsCollection,
                            geotiffCollection,
                            renderer->getElevationDataTileSize()
                        )
                    );
                    heightmap = true;
                }
                else if (heightsCollection)
                {
                    renderer->setElevationDataProvider(
                        std::make_shared<OsmAnd::SqliteHeightmapTileProvider>(
                            heightsCollection,
                            renderer->getElevationDataTileSize()
                        )
                    );
                }
                else if (geotiffCollection)
                {
                    renderer->setElevationDataProvider(
                        std::make_shared<OsmAnd::SqliteHeightmapTileProvider>(
                            geotiffCollection,
                            renderer->getElevationDataTileSize()
                        )
                    );
                    heightmap = true;
                }
                if (heightsCollection || geotiffCollection)
                {
                    elevationConfigurationPresetIndex = 0;
                    renderer->setElevationConfiguration(elevationConfigurationPresets[elevationConfigurationPresetIndex].second);
                }
            }
        }
        else
        {
            if (modifiers & GLUT_ACTIVE_CTRL)
            {
                if (--elevationConfigurationPresetIndex < 0)
                {
                    elevationConfigurationPresetIndex += elevationConfigurationPresetsCount;
                }
            }
            else
            {
                elevationConfigurationPresetIndex = (elevationConfigurationPresetIndex + 1) % elevationConfigurationPresetsCount;
            }
            renderer->setElevationConfiguration(elevationConfigurationPresets[elevationConfigurationPresetIndex].second);
        }
        return;
    case 'E':
    {
        if (modifiers & GLUT_ACTIVE_ALT)
        {
            auto configuration = state.elevationConfiguration;
            const auto step = 0.5f;
            if (modifiers & GLUT_ACTIVE_CTRL)
            {
                configuration.visualizationZ -= step;
            }
            else
            {
                configuration.visualizationZ += step;
            }
            renderer->setElevationConfiguration(configuration);
        }
        else
        {
            auto configuration = state.elevationConfiguration;
            const auto step = 0.1f;
            if (modifiers & GLUT_ACTIVE_CTRL)
            {
                configuration.visualizationAlpha += step;
            }
            else
            {
                configuration.visualizationAlpha -= step;
            }
            renderer->setElevationConfiguration(configuration);
        }
        return;
    }
    case 'T':
    case 't':
    {
        renderer->setDetailedDistance(state.detailedDistance + (key == 'T' ? 10.0f : 1.0f));
        return;
    }
    case 'G':
    case 'g':
    {
        renderer->setDetailedDistance(state.detailedDistance - (key == 'G' ? 10.0f : 1.0f));
        return;
    }
    case 'i':
        renderer->setFieldOfView(state.fieldOfView + 0.5f);
        return;
    case 'k':
        renderer->setFieldOfView(state.fieldOfView - 0.5f);
        return;
    case 'o':
    {
        auto position31 = renderer->getState().target31;
        OsmAnd::ReverseGeocoder reverseGeocoder{obfsCollection, roadLocator};
        OsmAnd::ReverseGeocoder::Criteria criteria;
        criteria.position31 = position31;
        QStringList roads{};
        reverseGeocoder.performSearch(
                    criteria,
                    [&roads](const OsmAnd::ISearch::Criteria& criteria,
                    const OsmAnd::BaseSearch::IResultEntry& resultEntry) {
            roads.append(static_cast<const OsmAnd::ReverseGeocoder::ResultEntry&>(resultEntry).toString());
        });
        textInfoDialog("Reverse geocoding", roads.join("\n"));
        return;
    }
    case 'l':
    {
        auto latLon = OsmAnd::Utilities::convert31ToLatLon(state.target31);
        auto text = inputDialog(QStringLiteral("Input coordinate"), QStringLiteral("Coordinate: "), latLon.toQString());
        latLon = OsmAnd::CoordinateSearch::search(text);
        auto target31 = OsmAnd::Utilities::convertLatLonTo31(latLon);
        renderer->setMapTargetLocation(target31);
        return;
    }
    case 'c':
    {
        auto config = renderer->getConfiguration();
        config->limitTextureColorDepthBy16bits = !config->limitTextureColorDepthBy16bits;
        renderer->setConfiguration(config);
        return;
    }
    case 'b':
    {
        auto config = renderer->getConfiguration();
        config->texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Normal;
        renderer->setConfiguration(config);
        return;
    }
    case 'n':
    {
        auto config = renderer->getConfiguration();
        config->texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Good;
        renderer->setConfiguration(config);
        return;
    }
    case 'm':
    {
        auto config = renderer->getConfiguration();
        config->texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Best;
        renderer->setConfiguration(config);
        return;
    }
    case 'p':
        std::async(std::launch::async, [=]
        {
            const auto downloadProgress =
                []
            (const uint64_t bytesDownloaded, const uint64_t bytesTotal)
            {
                std::cout << "... downloaded " << bytesDownloaded << " of " << bytesTotal << " (" << ((bytesDownloaded * 100) / bytesTotal) << "%)" << std::endl;
            };

            resourcesManager->updateRepository();

            if (!resourcesManager->isResourceInstalled(QLatin1String("world_basemap.map.obf")))
                resourcesManager->installFromRepository(QLatin1String("world_basemap.map.obf"), downloadProgress);
            else if (resourcesManager->isInstalledResourceOutdated(QLatin1String("world_basemap.map.obf")))
                resourcesManager->updateFromRepository(QLatin1String("world_basemap.map.obf"), downloadProgress);

            if (!resourcesManager->isResourceInstalled(QLatin1String("ukraine_europe.map.obf")))
                resourcesManager->installFromRepository(QLatin1String("ukraine_europe.map.obf"), downloadProgress);
            else if (resourcesManager->isInstalledResourceOutdated(QLatin1String("ukraine_europe.map.obf")))
                resourcesManager->updateFromRepository(QLatin1String("ukraine_europe.map.obf"), downloadProgress);

            if (!resourcesManager->isResourceInstalled(QLatin1String("netherlands_europe.map.obf")))
                resourcesManager->installFromRepository(QLatin1String("netherlands_europe.map.obf"), downloadProgress);
            else if (resourcesManager->isInstalledResourceOutdated(QLatin1String("netherlands_europe.map.obf")))
                resourcesManager->updateFromRepository(QLatin1String("netherlands_europe.map.obf"), downloadProgress);
        });
        return;
    case 'h':
    {
        if (mapObjectsSymbolsProvider && renderer->removeSymbolsProvider(mapObjectsSymbolsProvider))
        {
            mapObjectsSymbolsProvider.reset();
        }
        else
        {
            if (!binaryMapObjectsProvider)
                binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));
            mapPrimitivesProvider.reset(new OsmAnd::MapPrimitivesProvider(binaryMapObjectsProvider, primitivizer));

            mapObjectsSymbolsProvider.reset(new OsmAnd::MapObjectsSymbolsProvider(mapPrimitivesProvider, 256u, nullptr, false));
            renderer->addSymbolsProvider(mapObjectsSymbolsProvider);
        }
        return;
    }
    case '.':
    {
        if (amenitySymbolsProvider && renderer->removeSymbolsProvider(amenitySymbolsProvider))
        {
            amenitySymbolsProvider.reset();
        }
        else
        {
            amenitySymbolsProvider.reset(new OsmAnd::AmenitySymbolsProvider(obfsCollection, 1.0f, 256.0f));
            renderer->addSymbolsProvider(amenitySymbolsProvider);
        }
        return;
    }
    case 'q':
        animator->pause();
        animator->cancelAllAnimations();
        animator->animateAzimuthTo(0.0f, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
        animator->resume();
        return;
    case 'v':
    {
        OsmAnd::AddressesByNameSearch addressByNameSearch{obfsCollection};
        OsmAnd::AddressesByNameSearch::Criteria criteria;
        criteria.name = inputDialog(QStringLiteral("Input name"), QStringLiteral("Name: "));
        QStringList result{};
        addressByNameSearch.performSearch(
                    criteria,
                    [&result](const OsmAnd::ISearch::Criteria& criteria,
                    const OsmAnd::BaseSearch::IResultEntry& resultEntry) {
            result.append(static_cast<const OsmAnd::AddressesByNameSearch::ResultEntry&>(resultEntry).address->toString());
        });
        textInfoDialog("Search results", result.join("\n"));
        return;
    }
    case 'z':
    {
        auto text = inputDialog(QStringLiteral("Input zoom"), QStringLiteral("Zoom: "), QString::number(state.surfaceZoomLevel));
        bool ok;
        double zoom = text.toDouble(&ok);
        if (ok)
            renderer->setZoom(zoom);
        return;
    }
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
    {
        auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? 1 : 0;
        activateProvider(layerId, key - '0');
        return;
    }
    case '[':
        if (renderer->isSymbolsUpdateSuspended())
            while (!renderer->resumeSymbolsUpdate());
        else
            while (!renderer->suspendSymbolsUpdate());
        return;
    case ']':
        if (renderer->isGpuWorkerPaused())
            while (!renderer->resumeGpuWorker());
        else
            while (!renderer->suspendGpuWorker());
        return;
    case '\\':
    {
        auto settings = renderer->getDebugSettings();
        settings->mapLayersBatchingForbidden = !settings->mapLayersBatchingForbidden;
        renderer->setDebugSettings(settings);
        return;
    }
    case ' ':
    {
        const OsmAnd::PointI amsterdam(
            1102430866,
            704978668);
        const OsmAnd::PointI kiev(
            1255337783,
            724166131);
        static bool flag = false;
        const auto& target = flag ? amsterdam : kiev;
        flag = !flag;

        animator->pause();
        animator->cancelAllAnimations();
        animator->animateMoveTo(target, 1.0f, false, false, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
        animator->resume();
        return;
    }
    case '-':
        if (modifiers & GLUT_ACTIVE_SHIFT)
        {
            const auto zoom = state.surfaceZoomLevel + (state.surfaceVisualZoom >= 1.0f ? state.surfaceVisualZoom - 1.0f : (state.surfaceVisualZoom - 1.0f) * 2.0f);
            renderer->setZoom(zoom - 1.0f);
        }
        else
        {
            animator->pause();
            animator->cancelAllAnimations();
            animator->animateZoomBy(-1.0f, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
            animator->resume();
        }
        return;
    case '+':
        if (modifiers & GLUT_ACTIVE_SHIFT)
        {
            const auto zoom = state.surfaceZoomLevel + (state.surfaceVisualZoom >= 1.0f ? state.surfaceVisualZoom - 1.0f : (state.surfaceVisualZoom - 1.0f) * 2.0f);
            renderer->setZoom(zoom + 1.0f);
        }
        else
        {
            animator->pause();
            animator->cancelAllAnimations();
            animator->animateZoomBy(+1.0f, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
            animator->resume();
        }
        return;
    }

    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Warning,
        "Unknown hot-key '%c' (%u) (ctrl%s, alt%s, shift%s)",
        key,
        static_cast<unsigned int>(key),
        modifiers & GLUT_ACTIVE_CTRL ? "+" : "-",
        modifiers & GLUT_ACTIVE_ALT ? "+" : "-",
        modifiers & GLUT_ACTIVE_SHIFT ? "+" : "-");
}

void specialHandler(int key, int x, int y)
{
    auto modifiers = glutGetModifiers();
#if defined(OSMAND_TARGET_OS_macosx)
    x11AlterModifiers(modifiers);
#endif // defined(OSMAND_TARGET_OS_macosx)

    const auto state = renderer->getState();
    const auto step = (modifiers & GLUT_ACTIVE_SHIFT) ? 1.0f : 0.1f;

    switch (key)
    {
    case GLUT_KEY_F5:
        renderer->forcedFrameInvalidate();
        return;
    case GLUT_KEY_F6:
        renderer->forcedGpuProcessingCycle();
        return;
    case GLUT_KEY_F2:
        renderer->dumpResourcesInfo();
        return;
    case GLUT_KEY_LEFT:
        renderer->setAzimuth(state.azimuth + step);
        return;
    case GLUT_KEY_RIGHT:
        renderer->setAzimuth(state.azimuth - step);
        return;
    case GLUT_KEY_UP:
        renderer->setElevationAngle(state.elevationAngle + step);
        return;
    case GLUT_KEY_DOWN:
        renderer->setElevationAngle(state.elevationAngle - step);
        return;
    }

    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Warning,
        "Unknown hot-key %d (ctrl%s, alt%s, shift%s)",
        key,
        modifiers & GLUT_ACTIVE_CTRL ? "+" : "-",
        modifiers & GLUT_ACTIVE_ALT ? "+" : "-",
        modifiers & GLUT_ACTIVE_SHIFT ? "+" : "-");
}

void closeHandler()
{
    renderer->releaseRendering(true);

#if defined(OSMAND_TARGET_OS_macosx)
    x11Release();
#endif // defined(OSMAND_TARGET_OS_macosx)
}

void activateProvider(int layerIdx, int idx)
{
    if (idx == 0)
    {
        renderer->resetMapLayerProvider(layerIdx);
    }
    else if (idx == 1)
    {
        auto tileProvider = OsmAnd::OnlineTileSources::getBuiltIn()->createProviderFor(OsmAnd::OnlineTileSources::BuiltInOsmAndHD);
        renderer->setMapLayerProvider(layerIdx, tileProvider);
    }
    else if (idx == 2)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider = std::make_shared<OsmAnd::ObfMapObjectsProvider>(obfsCollection, obfMapObjectsProviderMode);
        mapPrimitivesProvider = std::make_shared<OsmAnd::MapPrimitivesProvider>(binaryMapObjectsProvider, primitivizer);

        // general
        QHash< QString, QString > settings;
        settings.insert("baseAppMode", "default");
        //settings.insert("contourLines", "11");
        // settings.insert("osmcTraces", "true");
//        settings.insert("OSMMapperAssistantFixme", "true");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 3)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider = std::make_shared<OsmAnd::ObfMapObjectsProvider>(obfsCollection, obfMapObjectsProviderMode);
        mapPrimitivesProvider = std::make_shared<OsmAnd::MapPrimitivesProvider>(binaryMapObjectsProvider, primitivizer);

        // car
        QHash< QString, QString > settings;
        settings.insert("baseAppMode", "car");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 4)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider = std::make_shared<OsmAnd::ObfMapObjectsProvider>(obfsCollection, obfMapObjectsProviderMode);
        mapPrimitivesProvider = std::make_shared<OsmAnd::MapPrimitivesProvider>(binaryMapObjectsProvider, primitivizer);

        // bicycle
        QHash< QString, QString > settings;
        settings.insert("baseAppMode", "bicycle");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 5)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider = std::make_shared<OsmAnd::ObfMapObjectsProvider>(obfsCollection, obfMapObjectsProviderMode);
        mapPrimitivesProvider = std::make_shared<OsmAnd::MapPrimitivesProvider>(binaryMapObjectsProvider, primitivizer);

        // pedestrian
        QHash< QString, QString > settings;
        settings.insert("baseAppMode", "pedestrian");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 6)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider = std::make_shared<OsmAnd::ObfMapObjectsProvider>(obfsCollection, obfMapObjectsProviderMode);

        auto tileProvider = new OsmAnd::ObfMapObjectsMetricsLayerProvider(binaryMapObjectsProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 7)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider = std::make_shared<OsmAnd::ObfMapObjectsProvider>(obfsCollection, obfMapObjectsProviderMode);
        mapPrimitivesProvider = std::make_shared<OsmAnd::MapPrimitivesProvider>(binaryMapObjectsProvider, primitivizer);

        auto tileProvider = new OsmAnd::MapPrimitivesMetricsLayerProvider(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 8)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider = std::make_shared<OsmAnd::ObfMapObjectsProvider>(obfsCollection, obfMapObjectsProviderMode);
        mapPrimitivesProvider = std::make_shared<OsmAnd::MapPrimitivesProvider>(binaryMapObjectsProvider, primitivizer);

        auto tileProvider = new OsmAnd::MapRasterMetricsLayerProvider(
            std::shared_ptr<OsmAnd::MapRasterLayerProvider>(new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider)));
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 9)
    {
        if (geotiffDirSpecified)
        {
            if (!hillshade)
            {
                if (!geotiffCollection)
                {
                    const auto manualGeoTiffCollection = new OsmAnd::GeoTiffCollection();
                    manualGeoTiffCollection->addDirectory(geotiffDir);
                    manualGeoTiffCollection->setLocalCache(cacheDir);
                    geotiffCollection.reset(manualGeoTiffCollection);
                }
                auto reliefRasterMapLayerProvider = new OsmAnd::HillshadeRasterMapLayerProvider(geotiffCollection,
                    QString("../../../../tools/obf-generation/heightmap/color/hillshade_main.txt"),
                    QString("../../../../tools/obf-generation/heightmap/color/color_slope.txt"));
                //auto reliefRasterMapLayerProvider = new OsmAnd::SlopeRasterMapLayerProvider(geotiffCollection,
                //    QString("../../../../tools/obf-generation/heightmap/color/slopes_main.txt"));

                renderer->setMapLayerProvider(layerIdx,
                    std::shared_ptr<OsmAnd::IMapLayerProvider>(reliefRasterMapLayerProvider));
                hillshade = true;
            }
            else
            {
                if (!heightmap)
                    geotiffCollection.reset();
                renderer->resetMapLayerProvider(layerIdx);
                hillshade = false;
            }
        }
    }
    //else if (idx == 7)
    //{
    //    //        auto hillshadeTileProvider = new OsmAnd::HillshadeTileProvider();
    //    //        renderer->setTileProvider(layerId, hillshadeTileProvider);
    //}
}

void idleHandler(void)
{
    static std::chrono::time_point<std::chrono::high_resolution_clock> lastTimeStamp;
    static bool lastTimeStampInitialized = false;

    if (!lastTimeStampInitialized)
    {
        lastTimeStamp = std::chrono::high_resolution_clock::now();
        lastTimeStampInitialized = true;
        return;
    }

    auto currentTimeStamp = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float> elapsedSeconds = currentTimeStamp - lastTimeStamp;

    animator->update(elapsedSeconds.count());

    lastTimeStamp = currentTimeStamp;
     // sleep 10 ms
     usleep(10 * 1000);
}

void displayHandler()
{
    (void)glGetError();
    glPolygonMode(GL_FRONT_AND_BACK, renderWireframe ? GL_LINE : GL_FILL);
    verifyOpenGL();
    //////////////////////////////////////////////////////////////////////////

    OsmAnd::Stopwatch totalStopwatch(true);

    OsmAnd::IMapRenderer_Metrics::Metric_update updateMetrics;
    OsmAnd::IMapRenderer_Metrics::Metric_prepareFrame prepareMetrics;
    OsmAnd::AtlasMapRenderer_Metrics::Metric_renderFrame renderMetrics;

    renderer->update(&updateMetrics);
    if (renderer->prepareFrame(&prepareMetrics))
        renderer->renderFrame(&renderMetrics);

    verifyOpenGL();

    //////////////////////////////////////////////////////////////////////////
    if (!use43 && !nSight)
    {
        const auto state = renderer->getState();
        const auto configuration = renderer->getConfiguration();

        glEnable(GL_BLEND);
        glBlendFunc(GL_SRC_ALPHA, GL_ONE_MINUS_SRC_ALPHA);

        glDisable(GL_DEPTH_TEST);

        glMatrixMode(GL_PROJECTION);
        glLoadIdentity();
        gluOrtho2D(0, viewport.width(), 0, viewport.height());

        glMatrixMode(GL_MODELVIEW);
        glLoadIdentity();

        glPolygonMode(GL_FRONT_AND_BACK, GL_FILL);

        auto w = 390;
        auto h1 = 15.5f * 31;
        auto t = viewport.height();
        glColor4f(0.5f, 0.5f, 0.5f, 0.6f);
        glBegin(GL_QUADS);
        glVertex2f(0.0f, t);
        glVertex2f(w, t);
        glVertex2f(w, t - h1);
        glVertex2f(0.0f, t - h1);
        glEnd();
        verifyOpenGL();

#if USE_GREEN_TEXT_COLOR
        glColor3f(0.0f, 1.0f, 0.0f);
#else
        glColor3f(0.2f, 0.2f, 0.2f);
#endif
        auto line = 0;

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fov (keys i,k)         : %1").arg(state.fieldOfView)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("visible distance (f,r) : %1").arg(state.visibleDistance)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("detailed distance (g,t): %1").arg(state.detailedDistance)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("azimuth (arrows l,r)   : %1").arg(state.azimuth)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("pitch (arrows u,d)     : %1").arg(state.elevationAngle)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("target (keys w,a,s,d,l): %1 %2").arg(state.target31.x).arg(state.target31.y)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("zoom (mouse wheel)     : %1").arg(state.surfaceZoomLevel + (state.surfaceVisualZoom >= 1.0f ? state.surfaceVisualZoom - 1.0f : (state.surfaceVisualZoom - 1.0f) * 2.0f))));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("zoom level (key z)     : %1").arg(state.surfaceZoomLevel)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("visual zoom (+ shift)  : %1 + %2").arg(state.visualZoom).arg(state.visualZoomShift)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("camera latitude        : %1").arg(std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getCameraCoordinates().latitude)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("camera longitude       : %1").arg(std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getCameraCoordinates().longitude)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("camera height (meters) : %1").arg(std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getCameraHeight())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("visible tiles          : %1").arg(std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getAllTilesCount())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("high-detailed tiles    : %1").arg(std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getVisibleTilesCount())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("detail levels          : %1").arg(std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getDetailLevelsCount())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("wireframe (key x)      : %1").arg(renderWireframe)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("elevation (key alt+e)  : %1").arg((bool)state.elevationDataProvider)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString(" -> preset (key e)     : %1").arg(elevationConfigurationPresets[elevationConfigurationPresetIndex].first)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString(" -> alpha (key E)      : %1").arg(state.elevationConfiguration.visualizationAlpha)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString(" -> z (key alt+E)      : %1").arg(state.elevationConfiguration.visualizationZ)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QStringLiteral("reverse geocoding (key o)")));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("16-bit textures (key c): %1").arg(configuration->limitTextureColorDepthBy16bits)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("tex-filtering (b,n,m)  : %1").arg(static_cast<int>(configuration->texturesFilteringQuality))));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("symbols (key h)        : %1").arg(!state.keyedSymbolsProviders.isEmpty() || !state.tiledSymbolsProviders.isEmpty())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("symbols loaded         : %1").arg(renderer->getSymbolsCount())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("symbols suspended ([)  : %1").arg(renderer->isSymbolsUpdateSuspended())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("GPU worker paused (])  : %1").arg(renderer->isGpuWorkerPaused())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Raster batching off (\\): %1").arg(renderer->getDebugSettings()->mapLayersBatchingForbidden)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * (++line));
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("# of resource tasks    : %1").arg(renderer->getActiveResourceRequestsCount())));
        verifyOpenGL();

        glColor4f(0.5f, 0.5f, 0.5f, 0.6f);
        glBegin(GL_QUADS);
        glVertex2f(0.0f, 16 * 13);
        glVertex2f(w, 16 * 13);
        glVertex2f(w, 0.0f);
        glVertex2f(0.0f, 0.0f);
        glEnd();
        verifyOpenGL();

#if USE_GREEN_TEXT_COLOR
        glColor3f(0.0f, 1.0f, 0.0f);
#else
        glColor3f(0.2f, 0.2f, 0.2f);
#endif
        glRasterPos2f(8, 16 * 12);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Last clicked tile: (%1, %2)@%3")
                .arg(lastClickedLocation31.x >> (31 - state.zoomLevel))
                .arg(lastClickedLocation31.y >> (31 - state.zoomLevel))
                .arg(state.zoomLevel)));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 11);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Last clicked location: %1 %2").arg(lastClickedLocation31.x).arg(lastClickedLocation31.y)));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 10);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Tile providers (holding alt controls overlay0):")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 9);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("0 - disable")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 8);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("1 - Mapnik (OsmAnd)")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 7);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("2 - Offline maps [General]")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 6);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("3 - Offline maps [Car]")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 5);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("4 - Offline maps [Bicycle]")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 4);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("5 - Offline maps [Pedestrian]")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 3);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("6 - Metrics [Binary Map Data Provider]")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 2);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("7 - Metrics [Binary Map Primitives Provider]")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 1);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("8 - Metrics [Binary Map Raster Tile Provider]")));
        verifyOpenGL();
    }

    glFlush();

    if (false)
    {
        std::array<float, SCREEN_WIDTH * SCREEN_HEIGHT> depthBuffer;
        glPixelStorei(GL_PACK_ROW_LENGTH, 0);
        glReadPixels(0, 0, SCREEN_WIDTH, SCREEN_HEIGHT, GL_DEPTH_COMPONENT, GL_FLOAT, depthBuffer.data());
        verifyOpenGL();

        const auto maxDepth = *std::max_element(depthBuffer.cbegin(), depthBuffer.cend());
        const auto minDepth = *std::min_element(depthBuffer.cbegin(), depthBuffer.cend());

        std::array<OsmAnd::ColorARGB, SCREEN_WIDTH * SCREEN_HEIGHT> visualDepthBuffer;
        for (auto yRow = 0; yRow < SCREEN_HEIGHT; yRow++)
        {
            for (auto xCol = 0; xCol < SCREEN_WIDTH; xCol++)
            {
                const auto depthValue = depthBuffer[yRow * SCREEN_WIDTH + xCol];
                const auto visualDepthValue = static_cast<uint8_t>(std::clamp(depthValue * 255.0f, 0.0f, 255.0f));
                visualDepthBuffer[yRow * SCREEN_WIDTH + xCol] = OsmAnd::ColorARGB(127, visualDepthValue, 0, 0);
            }
        }

        glRasterPos2f(0, 0);
        glPixelStorei(GL_UNPACK_ROW_LENGTH, 0);
        glDrawPixels(SCREEN_WIDTH, SCREEN_HEIGHT, GL_BGRA, GL_UNSIGNED_BYTE, visualDepthBuffer.data());
        verifyOpenGL();

        glFlush();
    }

    glutSwapBuffers();

    if (nSight || gDEBugger || constantRefresh)
        glutPostRedisplay();
    if (gDEBugger)
        glFrameTerminatorGREMEDY();
}

void verifyOpenGL()
{
    auto result = glGetError();
    if (result == GL_NO_ERROR)
        return;

    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Error, "Host OpenGL error 0x%08x : %s\n", result, gluErrorString(result));
}
