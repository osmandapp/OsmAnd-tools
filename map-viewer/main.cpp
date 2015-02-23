#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <memory>
#include <chrono>
#include <future>
#include <fstream>

#include <GL/glew.h>
#include <GL/freeglut.h>

#include <SkImageDecoder.h>

#include <OsmAndCore/QtExtensions.h>
#include <QString>
#include <QList>
#include <QFile>
#include <QMutex>
#include <QElapsedTimer>

#include <OsmAndCore.h>
#include <OsmAndCore/Concurrent.h>
#include <OsmAndCore/Common.h>
#include <OsmAndCore/QuadTree.h>
#include <OsmAndCore/Utilities.h>
#include <OsmAndCore/Logging.h>
#include <OsmAndCore/Stopwatch.h>
#include <OsmAndCore/CoreResourcesEmbeddedBundle.h>
#include <OsmAndCore/ResourcesManager.h>
#include <OsmAndCore/ObfsCollection.h>
#include <OsmAndCore/ObfDataInterface.h>
#include <OsmAndCore/WorldRegions.h>
#include <OsmAndCore/RoadLocator.h>
#include <OsmAndCore/Data/Road.h>
#include <OsmAndCore/Data/ObfRoutingSectionInfo.h>
#include <OsmAndCore/Map/MapRasterizer.h>
#include <OsmAndCore/Map/MapPresentationEnvironment.h>
#include <OsmAndCore/Map/IMapStylesCollection.h>
#include <OsmAndCore/Map/MapStylesCollection.h>
#include <OsmAndCore/Map/IMapStylesPresetsCollection.h>
#include <OsmAndCore/Map/MapStyleEvaluator.h>
#include <OsmAndCore/Map/IMapRenderer.h>
#include <OsmAndCore/Map/IMapRenderer_Metrics.h>
#include <OsmAndCore/Map/IAtlasMapRenderer.h>
#include <OsmAndCore/Map/AtlasMapRendererConfiguration.h>
#include <OsmAndCore/Map/AtlasMapRenderer_Metrics.h>
#include <OsmAndCore/Map/OnlineRasterMapLayerProvider.h>
#include <OsmAndCore/Map/OnlineTileSources.h>
#include <OsmAndCore/Map/HillshadeTileProvider.h>
#include <OsmAndCore/Map/IMapElevationDataProvider.h>
#include <OsmAndCore/Map/HeightmapTileProvider.h>
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
#include <OsmAndCore/FavoriteLocationsGpxCollection.h>
#include <OsmAndCore/Map/FavoriteLocationsPresenter.h>
#include <OsmAndCore/GeoInfoDocument.h>
#include <OsmAndCore/GpxDocument.h>
#include <OsmAndCore/Map/GeoInfoPresenter.h>
#include <OsmAndCore/Search/InAreaSearchEngine.h>
#include <OsmAndCore/Search/InAreaSearchSession.h>
#include <OsmAndCore/Search/PoiSearchDataSource.h>

bool glutWasInitialized = false;
QMutex glutWasInitializedFlagMutex;

OsmAnd::AreaI viewport;
std::shared_ptr<OsmAnd::IMapRenderer> renderer;
std::shared_ptr<OsmAnd::ResourcesManager> resourcesManager;
std::shared_ptr<const OsmAnd::IObfsCollection> obfsCollection;
std::shared_ptr<OsmAnd::ObfMapObjectsProvider> binaryMapObjectsProvider;
std::shared_ptr<OsmAnd::MapPresentationEnvironment> mapPresentationEnvironment;
std::shared_ptr<OsmAnd::MapPrimitiviser> primitivizer;
std::shared_ptr<OsmAnd::MapPrimitivesProvider> mapPrimitivesProvider;
std::shared_ptr<const OsmAnd::IMapStylesCollection> stylesCollection;
std::shared_ptr<const OsmAnd::ResolvedMapStyle> style;
std::shared_ptr<OsmAnd::MapAnimator> animator;
std::shared_ptr<OsmAnd::MapObjectsSymbolsProvider> mapObjectsSymbolsProvider;
std::shared_ptr<OsmAnd::MapMarkersCollection> markers;
std::shared_ptr<OsmAnd::FavoriteLocationsGpxCollection> favorites;
std::shared_ptr<OsmAnd::FavoriteLocationsPresenter> favoritesPresenter;
std::shared_ptr<OsmAnd::MapMarker> lastClickedLocationMarker;
std::shared_ptr<OsmAnd::RoadLocator> roadLocator;
std::shared_ptr<OsmAnd::GeoInfoPresenter> gpxPresenter;
//const auto obfMapObjectsProviderMode = OsmAnd::ObfMapObjectsProvider::Mode::OnlyBinaryMapObjects;
const auto obfMapObjectsProviderMode = OsmAnd::ObfMapObjectsProvider::Mode::BinaryMapObjectsAndRoads;

bool obfsDirSpecified = false;
QDir obfsDir;
bool dataDirSpecified = false;
QDir dataDir;
QDir cacheDir(QDir::current());
QDir heightsDir;
bool wasHeightsDirSpecified = false;
QFileInfoList styleFiles;
QString styleName = "default";

#define USE_GREEN_TEXT_COLOR 1

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
    coreResourcesEmbeddedBundle = OsmAnd::CoreResourcesEmbeddedBundle::loadFromLibrary(QLatin1String("OsmAndCore_ResourcesBundle_shared"));
#endif // defined(OSMAND_CORE_STATIC)
    if (!OsmAnd::InitializeCore(coreResourcesEmbeddedBundle))
    {
        std::cerr << "Failed to initialize core" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }
    std::cout << "Initialized Core" << std::endl;
    
    //////////////////////////////////////////////////////////////////////////

    const std::unique_ptr<SkImageDecoder> pngDecoder(CreatePNGImageDecoder());

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
            wasHeightsDirSpecified = true;
        }
        else if (arg == "-nsight")
        {
            useSpecificOpenGL = true;
            use43 = true;
            constantRefresh = true;
            nSight = true;
            gDEBugger = false;
        }
        else if (arg == "-gdebugger")
        {
            useSpecificOpenGL = true;
            use43 = false;
            constantRefresh = true;
            nSight = false;
            gDEBugger = true;
        }
    }

    if (!obfsDirSpecified && !dataDirSpecified)
    {
        std::cerr << "Nor OBFs directory nor data directory was specified" << std::endl;
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
        glutInitWindowSize(1024, 768);
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
    animator.reset(new OsmAnd::MapAnimator());
    animator->setMapRenderer(renderer);

    /*
    markers.reset(new OsmAnd::MapMarkersCollection());
    std::shared_ptr<OsmAnd::MapMarkerBuilder> markerBuilder(new OsmAnd::MapMarkerBuilder());
    {
        std::shared_ptr<SkBitmap> locationPinImage(new SkBitmap());
        pngDecoder->DecodeFile("d:\\OpenSource\\OsmAnd\\iOS7_icons_extended\\PNG\\Maps\\location\\location-32.png", locationPinImage.get());
        markerBuilder->setPinIcon(locationPinImage);
    }
    {
        std::shared_ptr<SkBitmap> locationOnMapSurfaceImage(new SkBitmap());
        pngDecoder->DecodeFile("d:\\OpenSource\\OsmAnd\\iOS7_icons_extended\\PNG\\Camping_Equipment\\campfire\\campfire-32.png", locationOnMapSurfaceImage.get());
        markerBuilder->addOnMapSurfaceIcon(reinterpret_cast<OsmAnd::MapMarker::OnSurfaceIconKey>(1), locationOnMapSurfaceImage);
    }
    markerBuilder->setIsAccuracyCircleSupported(true);
    markerBuilder->setAccuracyCircleBaseColor(OsmAnd::FColorRGB(1.0f, 0.0f, 0.0f));
    lastClickedLocationMarker = markerBuilder->buildAndAddToCollection(markers);
    lastClickedLocationMarker->setOnMapSurfaceIconDirection(reinterpret_cast<OsmAnd::MapMarker::OnSurfaceIconKey>(1), Q_QNAN);
    lastClickedLocationMarker->setIsAccuracyCircleVisible(true);
    lastClickedLocationMarker->setAccuracyCircleRadius(2000.0);
    renderer->addSymbolsProvider(markers);
    */

    favorites.reset(new OsmAnd::FavoriteLocationsGpxCollection());
    favoritesPresenter.reset(new OsmAnd::FavoriteLocationsPresenter(favorites));
    renderer->addSymbolsProvider(favoritesPresenter);
    //favorites->loadFrom(QLatin1String("d:\\OpenSource\\OsmAnd\\favorites.gpx"));

    //////////////////////////////////////////////////////////////////////////

    QList< std::shared_ptr<const OsmAnd::GeoInfoDocument> > geoInfoDocs;
    if (QFile::exists(QLatin1String("track.gpx")))
        geoInfoDocs.append(OsmAnd::GpxDocument::loadFrom("track.gpx"));
    gpxPresenter.reset(new OsmAnd::GeoInfoPresenter(geoInfoDocs));
    
    //////////////////////////////////////////////////////////////////////////

   
    //////////////////////////////////////////////////////////////////////////
    //QHash< QString, std::shared_ptr<const OsmAnd::WorldRegions::WorldRegion> > worldRegions;
    //OsmAnd::WorldRegions("d:\\OpenSource\\OsmAnd\\OsmAnd\\resources\\countries-info\\regions.ocbf").loadWorldRegions(worldRegions);
    //////////////////////////////////////////////////////////////////////////

    if (dataDirSpecified)
    {
        resourcesManager.reset(new OsmAnd::ResourcesManager(
            dataDir.absoluteFilePath(QLatin1String("storage")),
            dataDir.absoluteFilePath(QLatin1String("storage_ext")),
            QList<QString>(),
            dataDir.absoluteFilePath(QLatin1String("World_basemap_mini_2.obf")),
            dataDir.absoluteFilePath(QLatin1String("tmp"))));

        const auto renderer_ = renderer;
        resourcesManager->localResourcesChangeObservable.attach(nullptr,
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

        std::ofstream rulesOutput("rules.txt");
        if (rulesOutput)
        {
            rulesOutput << style->dump().toStdString() << std::endl;
            rulesOutput.close();
        }
    }

    //////////////////////////////////////////////////////////////////////////
    const std::shared_ptr<OsmAnd::InAreaSearchEngine> inAreaSearchEngine(new OsmAnd::InAreaSearchEngine());
    const std::shared_ptr<OsmAnd::PoiSearchDataSource> poiSearchDataSource(new OsmAnd::PoiSearchDataSource(obfsCollection));
    inAreaSearchEngine->addDataSource(poiSearchDataSource);

    const auto searchSession = std::static_pointer_cast<OsmAnd::InAreaSearchSession>(inAreaSearchEngine->createSession());

    searchSession->setArea(OsmAnd::AreaI64(OsmAnd::Utilities::boundingBox31FromAreaInMeters(5000, OsmAnd::PointI(
        1255337783,
        724166131))));
    searchSession->setQuery("Vadima");
    searchSession->startSearch();
    //////////////////////////////////////////////////////////////////////////

    roadLocator.reset(new OsmAnd::RoadLocator(obfsCollection));
    mapPresentationEnvironment.reset(new OsmAnd::MapPresentationEnvironment(style, density, mapScale, symbolsScale, "ru"));
    primitivizer.reset(new OsmAnd::MapPrimitiviser(mapPresentationEnvironment));

    OsmAnd::MapRendererSetupOptions rendererSetup;
    rendererSetup.frameUpdateRequestCallback =
        []
        (const OsmAnd::IMapRenderer* const mapRenderer)
        {
            //QMutexLocker scopedLocker(&glutWasInitializedFlagMutex);

            if (glutWasInitialized)
                glutPostRedisplay();
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
    //debugSettings->debugStageEnabled = true;
    //debugSettings->excludeBillboardSymbolsFromProcessing = true;
    //debugSettings->excludeOnSurfaceSymbolsFromProcessing = true;
    //debugSettings->excludeOnPathSymbolsFromProcessing = true;
    /*debugSettings->skipSymbolsMinDistanceToSameContentFromOtherSymbolCheck = true;
    debugSettings->skipSymbolsIntersectionCheck = true;
    
    debugSettings->showSymbolsBBoxesRejectedByMinDistanceToSameContentFromOtherSymbolCheck = true;
    debugSettings->showSymbolsBBoxesRejectedByIntersectionCheck = true;
    debugSettings->showSymbolsBBoxesRejectedByPresentationMode = true;*/
    //debugSettings->showSymbolsBBoxesAcceptedByIntersectionCheck = true;
    //debugSettings->skipSymbolsPresentationModeCheck = true;
    //debugSettings->showOnPathSymbolsRenderablesPaths = true;
    ////debugSettings->showOnPath2dSymbolGlyphDetails = true;
    ////debugSettings->showOnPath3dSymbolGlyphDetails = true;
    //debugSettings->allSymbolsTransparentForIntersectionLookup = true;
    //debugSettings->showTooShortOnPathSymbolsRenderablesPaths = true;
    //debugSettings->showAllPaths = true;
    renderer->setDebugSettings(debugSettings);
    
    viewport.top() = 0;
    viewport.left() = 0;
    viewport.bottom() = 768;
    viewport.right() = 1024;
    renderer->setWindowSize(OsmAnd::PointI(1024, 768));
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
    renderer->setTarget(OsmAnd::PointI(
        1102430866,
        704978668));
    renderer->setZoom(13.0f);
    //renderer->setZoom(10.0f);
    //renderer->setZoom(4.0f);

    renderer->setTarget(OsmAnd::Utilities::convertLatLonTo31(OsmAnd::LatLon(
        55.75369,
        37.62030)));
    renderer->setZoom(19.0f);

    //// Kiev
    //renderer->setTarget(OsmAnd::PointI(
    //    1255337783,
    //    724166131));
    //renderer->setZoom(11.0f);
    //renderer->setZoom(16.0f);

    //// Bug
    //renderer->setTarget(OsmAnd::Utilities::convertLatLonTo31(OsmAnd::LatLon(
    //    55.7286,
    //    37.6409)));
    //renderer->setZoom(16.0f);

    //renderer->setTarget(OsmAnd::PointI(
    //    1102425455,
    //    706223457));
    //renderer->setZoom(11.787f);
    //
    //// Synthetic
    //renderer->setTarget(OsmAnd::Utilities::convertLatLonTo31(OsmAnd::LatLon(
    //    45.731606,
    //    36.528217)));
    //renderer->setZoom(17.0f);

    // Tokyo
    /*renderer->setTarget(OsmAnd::PointI(
        OsmAnd::Utilities::get31TileNumberX(139.6917),
        OsmAnd::Utilities::get31TileNumberY(35.689506)));
        renderer->setZoom(14.0f);*/

    // Netanya
    /*renderer->setTarget(OsmAnd::PointI(
        OsmAnd::Utilities::get31TileNumberX(34.85320),
        OsmAnd::Utilities::get31TileNumberY(32.32288)));
        renderer->setZoom(14.0f);*/

    renderer->setAzimuth(0.0f);

    auto renderConfig = renderer->getConfiguration();
    renderer->setConfiguration(renderConfig);

    if (lastClickedLocationMarker)
        lastClickedLocationMarker->setPosition(renderer->getState().target31);

    bool ok = renderer->initializeRendering();
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

void mouseHandler(int button, int state, int x, int y)
{
    const auto modifiers = glutGetModifiers();

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
                    favorites->createFavoriteLocation(lastClickedLocation31);
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
                    const auto name = road->captions[road->encodingDecodingRules->name_encodingRuleId];
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "Found road: %s", qPrintable(name));
                }
            }
            else
            {
                OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "No road found!");
            }

            // Map symbol
            const auto mapSymbols = renderer->getSymbolsAt(OsmAnd::PointI(x, y));
            if (mapSymbols.isEmpty())
                OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, "No symbols found!");
            for (const auto& mapSymbol : constOf(mapSymbols))
            {
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
                    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info, " - from %s", qPrintable(group->toString()));
                }
            }
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
            std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getCurrentTileSizeOnScreenInPixels();

        OsmAnd::PointI newTarget;
        newTarget.x = dragInitTarget.x - static_cast<int32_t>(nx * scale31);
        newTarget.y = dragInitTarget.y - static_cast<int32_t>(ny * scale31);

        renderer->setTarget(newTarget);
        if (lastClickedLocationMarker)
            lastClickedLocationMarker->setPosition(newTarget);
    }
}

void mouseWheelHandler(int button, int dir, int x, int y)
{
    const auto modifiers = glutGetModifiers();
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

        const auto zoom = state.zoomLevel + (state.visualZoom >= 1.0f ? state.visualZoom - 1.0f : (state.visualZoom - 1.0f) * 2.0f);
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
    const auto modifiers = glutGetModifiers();
    const auto state = renderer->getState();
    const auto wasdZoom = static_cast<int>(
        state.zoomLevel + (state.visualZoom >= 1.0f ? state.visualZoom - 1.0f : (state.visualZoom - 1.0f) * 2.0f));
    const auto wasdStep = (1 << (31 - wasdZoom));

    switch (key)
    {
    case '\x1B':
        glutLeaveMainLoop();
        break;
    case 'W':
    case 'w':
    {
        auto newTarget = state.target31;
        newTarget.y -= wasdStep / (key == 'w' ? 50 : 10);
        renderer->setTarget(newTarget);
    }
        break;
    case 'S':
    case 's':
    {
        auto newTarget = state.target31;
        newTarget.y += wasdStep / (key == 's' ? 50 : 10);
        renderer->setTarget(newTarget);
    }
        break;
    case 'A':
    case 'a':
    {
        auto newTarget = state.target31;
        newTarget.x -= wasdStep / (key == 'a' ? 50 : 10);
        renderer->setTarget(newTarget);
    }
        break;
    case 'D':
    case 'd':
    {
        auto newTarget = state.target31;
        newTarget.x += wasdStep / (key == 'd' ? 50 : 10);
        renderer->setTarget(newTarget);
    }
        break;
    case 'r':
    {
        auto fogConfiguration = state.fogConfiguration;
        fogConfiguration.distanceToFog += 1.0f;
        renderer->setFogConfiguration(fogConfiguration);
        break;
    }
    case 'f':
    {
        auto fogConfiguration = state.fogConfiguration;
        fogConfiguration.distanceToFog += 1.0f;
        renderer->setFogConfiguration(fogConfiguration);
        break;
    }
    case 'x':
        renderWireframe = !renderWireframe;
        glutPostRedisplay();
        break;
    case 'e':
    {
        if (state.elevationDataProvider)
        {
            renderer->resetElevationDataProvider();
        }
        else
        {
            if (wasHeightsDirSpecified)
            {
                //auto provider = new OsmAnd::HeightmapTileProvider(heightsDir, cacheDir.absoluteFilePath(OsmAnd::HeightmapTileProvider::defaultIndexFilename));
                //renderer->setElevationDataProvider(std::shared_ptr<OsmAnd::IMapElevationDataProvider>(provider));
            }
        }
    }
        break;
    case 't':
    {
        auto fogConfiguration = state.fogConfiguration;
        fogConfiguration.density += 0.01f;
        renderer->setFogConfiguration(fogConfiguration);
        break;
    }
    case 'g':
    {
        auto fogConfiguration = state.fogConfiguration;
        fogConfiguration.density -= 0.01f;
        renderer->setFogConfiguration(fogConfiguration);
        break;
    }
    case 'u':
    {
        auto fogConfiguration = state.fogConfiguration;
        fogConfiguration.originFactor += 0.01f;
        renderer->setFogConfiguration(fogConfiguration);
        break;
    }
    case 'j':
    {
        auto fogConfiguration = state.fogConfiguration;
        fogConfiguration.originFactor -= 0.01f;
        renderer->setFogConfiguration(fogConfiguration);
        break;
    }
    case 'i':
        renderer->setFieldOfView(state.fieldOfView + 0.5f);
        break;
    case 'k':
        renderer->setFieldOfView(state.fieldOfView - 0.5f);
        break;
    //case 'o':
    //    renderer->setElevationDataScaleFactor(state.elevationDataScaleFactor + 0.1f);
    //    break;
    //case 'l':
    //    renderer->setElevationDataScaleFactor(state.elevationDataScaleFactor - 0.1f);
    //    break;
    case 'c':
    {
        auto config = renderer->getConfiguration();
        config->limitTextureColorDepthBy16bits = !config->limitTextureColorDepthBy16bits;
        renderer->setConfiguration(config);
    }
        break;
    case 'b':
    {
        auto config = renderer->getConfiguration();
        config->texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Normal;
        renderer->setConfiguration(config);
    }
        break;
    case 'n':
    {
        auto config = renderer->getConfiguration();
        config->texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Good;
        renderer->setConfiguration(config);
    }
        break;
    case 'm':
    {
        auto config = renderer->getConfiguration();
        config->texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Best;
        renderer->setConfiguration(config);
    }
        break;
    case 'p':
    {
        std::async(std::launch::async,
            [=]
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
    }
        break;
    case 'z':
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

            mapObjectsSymbolsProvider.reset(new OsmAnd::MapObjectsSymbolsProvider(mapPrimitivesProvider, 256u));
            renderer->addSymbolsProvider(mapObjectsSymbolsProvider);
        }
    }
        break;
    case 'q':
        animator->pause();
        animator->cancelAllAnimations();
        animator->animateAzimuthTo(0.0f, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
        animator->resume();
        break;
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
        break;
    }
    case '[':
        if (renderer->isSymbolsUpdateSuspended())
            while (!renderer->resumeSymbolsUpdate());
        else
            while (!renderer->suspendSymbolsUpdate());
        break;
    case ']':
        if (renderer->isGpuWorkerPaused())
            while (!renderer->resumeGpuWorker());
        else
            while (!renderer->suspendGpuWorker());
        break;
    case '\\':
    {
        auto settings = renderer->getDebugSettings();
        settings->mapLayersBatchingForbidden = !settings->mapLayersBatchingForbidden;
        renderer->setDebugSettings(settings);
        break;
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
    }
        break;
    case '-':
        animator->pause();
        animator->cancelAllAnimations();
        animator->animateZoomBy(-1.0f, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
        animator->resume();
        break;
    case '+':
        animator->pause();
        animator->cancelAllAnimations();
        animator->animateZoomBy(+1.0f, 1.0f, OsmAnd::MapAnimator::TimingFunction::EaseInOutQuadratic);
        animator->resume();
        break;
    case '*':
        break;
    }
}

void specialHandler(int key, int x, int y)
{
    const auto modifiers = glutGetModifiers();
    const auto state = renderer->getState();
    const auto step = (modifiers & GLUT_ACTIVE_SHIFT) ? 1.0f : 0.1f;

    switch (key)
    {
    case GLUT_KEY_F5:
        renderer->forcedFrameInvalidate();
        break;
    case GLUT_KEY_F6:
        renderer->forcedGpuProcessingCycle();
        break;
    case GLUT_KEY_F2:
        renderer->dumpResourcesInfo();
        break;
    case GLUT_KEY_LEFT:
        renderer->setAzimuth(state.azimuth + step);
        break;
    case GLUT_KEY_RIGHT:
        renderer->setAzimuth(state.azimuth - step);
        break;
    case GLUT_KEY_UP:
        renderer->setElevationAngle(state.elevationAngle + step);
        break;
    case GLUT_KEY_DOWN:
        renderer->setElevationAngle(state.elevationAngle - step);
        break;
    }
}

void closeHandler(void)
{
    renderer->releaseRendering(true);
}

void activateProvider(int layerIdx, int idx)
{
    if (idx == 0)
    {
        renderer->resetMapLayerProvider(layerIdx);
    }
    else if (idx == 1)
    {
        auto tileProvider = OsmAnd::OnlineTileSources::getBuiltIn()->createProviderFor(OsmAnd::OnlineTileSources::BuiltInOsmAndSD);
        renderer->setMapLayerProvider(layerIdx, tileProvider);
    }
    else if (idx == 2)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));
        mapPrimitivesProvider.reset(new OsmAnd::MapPrimitivesProvider(binaryMapObjectsProvider, primitivizer));

        // general
        QHash< QString, QString > settings;
        settings.insert("appMode", "browse map");
        //settings.insert("contourLines", "11");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));

        //OsmAnd::MapLayerConfiguration mapLayerConfiguration;
        //mapLayerConfiguration.opacity = 0.5f;
        //renderer->setMapLayerConfiguration(layerIdx, mapLayerConfiguration);
        //
        if (gpxPresenter)
        {
            const std::shared_ptr<OsmAnd::MapPrimitivesProvider> gpxPrimitivesProvider(new OsmAnd::MapPrimitivesProvider(
                gpxPresenter->createMapObjectsProvider(),
                primitivizer,
                256,
                OsmAnd::MapPrimitivesProvider::Mode::AllObjectsWithPolygonFiltering));
            auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(gpxPrimitivesProvider, false);
            renderer->setMapLayerProvider(10, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));

            mapObjectsSymbolsProvider.reset(new OsmAnd::MapObjectsSymbolsProvider(gpxPrimitivesProvider, 256u));
            renderer->addSymbolsProvider(mapObjectsSymbolsProvider);
        }
        //
    }
    else if (idx == 3)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));
        mapPrimitivesProvider.reset(new OsmAnd::MapPrimitivesProvider(binaryMapObjectsProvider, primitivizer));

        // car
        QHash< QString, QString > settings;
        settings.insert("appMode", "car");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 4)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));
        mapPrimitivesProvider.reset(new OsmAnd::MapPrimitivesProvider(binaryMapObjectsProvider, primitivizer));

        // bicycle
        QHash< QString, QString > settings;
        settings.insert("appMode", "bicycle");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 5)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));
        mapPrimitivesProvider.reset(new OsmAnd::MapPrimitivesProvider(binaryMapObjectsProvider, primitivizer));

        // pedestrian
        QHash< QString, QString > settings;
        settings.insert("appMode", "pedestrian");
        mapPresentationEnvironment->setSettings(settings);

        auto tileProvider = new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 6)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));

        auto tileProvider = new OsmAnd::ObfMapObjectsMetricsLayerProvider(binaryMapObjectsProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 7)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));
        mapPrimitivesProvider.reset(new OsmAnd::MapPrimitivesProvider(binaryMapObjectsProvider, primitivizer));

        auto tileProvider = new OsmAnd::MapPrimitivesMetricsLayerProvider(mapPrimitivesProvider);
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
    }
    else if (idx == 8)
    {
        if (!binaryMapObjectsProvider)
            binaryMapObjectsProvider.reset(new OsmAnd::ObfMapObjectsProvider(obfsCollection, obfMapObjectsProviderMode));
        mapPrimitivesProvider.reset(new OsmAnd::MapPrimitivesProvider(binaryMapObjectsProvider, primitivizer));

        auto tileProvider = new OsmAnd::MapRasterMetricsLayerProvider(
            std::shared_ptr<OsmAnd::MapRasterLayerProvider>(new OsmAnd::MapRasterLayerProvider_Software(mapPrimitivesProvider)));
        renderer->setMapLayerProvider(layerIdx, std::shared_ptr<OsmAnd::IMapLayerProvider>(tileProvider));
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

    const auto totalElapsed = totalStopwatch.elapsed();
    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Info,
        "Frame time %fs%s: update %d%%, prepare %d%%, render %d%%:\n%s\n%s\n%s",
        totalElapsed,
        totalElapsed > 0.033f ? qPrintable(QString(QLatin1String(" (%1 < 60fps)")).arg(static_cast<unsigned int>(1.0f / totalElapsed))) : "",
        static_cast<unsigned int>((updateMetrics.elapsedTime / totalElapsed) * 100),
        static_cast<unsigned int>((prepareMetrics.elapsedTime / totalElapsed) * 100),
        static_cast<unsigned int>((renderMetrics.elapsedTime / totalElapsed) * 100),
        qPrintable(updateMetrics.toString(false, QString(QLatin1String("update.")))),
        qPrintable(prepareMetrics.toString(false, QString(QLatin1String("prepare.")))),
        qPrintable(renderMetrics.toString(false, QString(QLatin1String("render.")))));
    
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
        auto h1 = 16 * 22;
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
        glRasterPos2f(8, t - 16 * 1);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fov (keys i,k)         : %1").arg(state.fieldOfView)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 2);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fog distance (keys r,f): %1").arg(state.fogConfiguration.distanceToFog)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 3);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("azimuth (arrows l,r)   : %1").arg(state.azimuth)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 4);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("pitch (arrows u,d)     : %1").arg(state.elevationAngle)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 5);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("target (keys w,a,s,d)  : %1 %2").arg(state.target31.x).arg(state.target31.y)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 6);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("zoom (mouse wheel)     : %1").arg(state.zoomLevel + (state.visualZoom >= 1.0f ? state.visualZoom - 1.0f : (state.visualZoom - 1.0f) * 2.0f))));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 7);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("zoom level             : %1").arg(state.zoomLevel)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 8);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("visual zoom (+ shift)  : %1 + %2").arg(state.visualZoom).arg(state.visualZoomShift)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 9);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("visible tiles          : %1").arg(std::dynamic_pointer_cast<OsmAnd::IAtlasMapRenderer>(renderer)->getVisibleTilesCount())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 10);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("wireframe (key x)      : %1").arg(renderWireframe)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 11);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("elevation data (key e) : %1").arg((bool)state.elevationDataProvider)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 12);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fog density (keys t,g) : %1").arg(state.fogConfiguration.density)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 13);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fog origin F (keys u,j): %1").arg(state.fogConfiguration.originFactor)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 14);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("height scale (keys o,l): %1").arg(state.elevationDataConfiguration.scaleFactor)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 15);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("16-bit textures (key c): %1").arg(configuration->limitTextureColorDepthBy16bits)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 16);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("tex-filtering (b,n,m)  : %1").arg(static_cast<int>(configuration->texturesFilteringQuality))));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 17);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("symbols (key z)        : %1").arg(!state.keyedSymbolsProviders.isEmpty() || !state.tiledSymbolsProviders.isEmpty())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 18);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("symbols loaded         : %1").arg(renderer->getSymbolsCount())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 19);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("symbols suspended ([)  : %1").arg(renderer->isSymbolsUpdateSuspended())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 20);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("GPU worker paused (])  : %1").arg(renderer->isGpuWorkerPaused())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 21);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Raster batching on (\\) : %1").arg(renderer->getDebugSettings()->mapLayersBatchingForbidden)));
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
            QString("Last clicked tile: (%1, %2)@%3").arg(lastClickedLocation31.x >> (31 - state.zoomLevel)).arg(lastClickedLocation31.y >> (31 - state.zoomLevel)).arg(state.zoomLevel)));
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
