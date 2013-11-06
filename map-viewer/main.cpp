/**
  * @file
  *
  * @section LICENSE
  *
  * OsmAnd - Android navigation software based on OSM maps.
  * Copyright (C) 2010-2013  OsmAnd Authors listed in AUTHORS file
  *
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * (at your option) any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.

  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */

#include <stdlib.h>
#include <memory>
#include <chrono>

#include <GL/glew.h>
#include <GL/freeglut.h>

#include <QString>
#include <QList>
#include <QFile>
#include <QMutex>

#include <OsmAndCore.h>
#include <OsmAndCore/Concurrent.h>
#include <OsmAndCore/Common.h>
#include <OsmAndCore/Utilities.h>
#include <OsmAndCore/Logging.h>
#include <OsmAndCore/Data/ObfsCollection.h>
#include <OsmAndCore/Data/ObfDataInterface.h>
#include <OsmAndCore/Map/Rasterizer.h>
#include <OsmAndCore/Map/RasterizerEnvironment.h>
#include <OsmAndCore/Map/MapStyles.h>
#include <OsmAndCore/Map/MapStyleEvaluator.h>
#include <OsmAndCore/Map/IMapRenderer.h>
#include <OsmAndCore/Map/OnlineMapRasterTileProvider.h>
#include <OsmAndCore/Map/HillshadeTileProvider.h>
#include <OsmAndCore/Map/IMapElevationDataProvider.h>
#include <OsmAndCore/Map/HeightmapTileProvider.h>
#include <OsmAndCore/Map/OfflineMapDataProvider.h>
#include <OsmAndCore/Map/OfflineMapRasterTileProvider_Software.h>
#include <OsmAndCore/Map/OfflineMapRasterTileProvider_GPU.h>
#include <OsmAndCore/Map/OfflineMapSymbolProvider.h>
#include <OsmAndCore/Map/MapAnimator.h>

bool glutWasInitialized = false;
QMutex glutWasInitializedFlagMutex;

OsmAnd::AreaI viewport;
std::shared_ptr<OsmAnd::IMapRenderer> renderer;
std::shared_ptr<OsmAnd::ObfsCollection> obfsCollection;
std::shared_ptr<OsmAnd::OfflineMapDataProvider> offlineMapDataProvider;
std::shared_ptr<OsmAnd::MapStyles> stylesCollection;
std::shared_ptr<OsmAnd::MapAnimator> animator;

QDir obfRoot(QDir::current());
QDir cacheDir(QDir::current());
QDir heightsDir;
bool wasHeightsDirSpecified = false;
QFileInfoList styleFiles;
QString styleName = "default";

bool use43 = false;

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
void activateProvider(OsmAnd::RasterMapLayerId layerId, int idx);
void verifyOpenGL();

OsmAnd::PointI lastClickedLocation31;

int main(int argc, char** argv)
{
    //////////////////////////////////////////////////////////////////////////
    OsmAnd::InitializeCore();

    for(int argIdx = 1; argIdx < argc; argIdx++)
    {
        const QString arg(argv[argIdx]);

        if (arg.startsWith("-stylesPath="))
        {
            auto path = arg.mid(strlen("-stylesPath="));
            QDir dir(path);
            if(!dir.exists())
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
            auto obfRootPath = arg.mid(strlen("-obfsDir="));
            obfRoot = QDir(obfRootPath);
            if(!obfRoot.exists())
            {
                std::cerr << "OBF directory does not exist" << std::endl;
                OsmAnd::ReleaseCore();
                return EXIT_FAILURE;
            }
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
    }
    
    // Obtain and configure rasterization style context
    std::shared_ptr<const OsmAnd::MapStyle> style;
    if(!styleName.isEmpty())
    {
        stylesCollection.reset(new OsmAnd::MapStyles());
        for(auto itStyleFile = styleFiles.begin(); itStyleFile != styleFiles.end(); ++itStyleFile)
        {
            const auto& styleFile = *itStyleFile;

            if(!stylesCollection->registerStyle(styleFile.absoluteFilePath()))
                std::cout << "Failed to parse metadata of '" << styleFile.fileName().toStdString() << "' or duplicate style" << std::endl;
        }
        if(!stylesCollection->obtainStyle(styleName, style))
        {
            std::cout << "Failed to resolve style '" << styleName.toStdString() << "'" << std::endl;
            OsmAnd::ReleaseCore();
            return EXIT_FAILURE;
        }
    }
    
    obfsCollection.reset(new OsmAnd::ObfsCollection());
    obfsCollection->watchDirectory(obfRoot);

#if defined(OSMAND_OPENGL_RENDERER_SUPPORTED)
    renderer = OsmAnd::createAtlasMapRenderer_OpenGL();
#endif
    if(!renderer)
    {
        std::cout << "No supported renderer" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }
    animator.reset(new OsmAnd::MapAnimator());
    animator->setMapRenderer(renderer);

    //////////////////////////////////////////////////////////////////////////

    {
        QMutexLocker scopedLocker(&glutWasInitializedFlagMutex);

        assert(glutInit != nullptr);
        glutInit(&argc, argv);

        glutSetOption(GLUT_ACTION_ON_WINDOW_CLOSE, GLUT_ACTION_CONTINUE_EXECUTION);
        glutInitWindowSize(1024, 768);
        glutInitDisplayMode(GLUT_DOUBLE | GLUT_RGBA | GLUT_DEPTH);
        if(!use43)
            glutInitContextVersion(3, 0);
        else
            glutInitContextVersion(4, 3);
        glutInitContextProfile(GLUT_CORE_PROFILE);
        assert(glutCreateWindow != nullptr);
        glutCreateWindow((const char*)xT("OsmAnd Bird : 3D map render tool"));

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
    OsmAnd::MapRendererSetupOptions rendererSetup;
    rendererSetup.frameRequestCallback = []()
    {
        //QMutexLocker scopedLocker(&glutWasInitializedFlagMutex);

        if(glutWasInitialized)
            glutPostRedisplay();
    };
    renderer->setup(rendererSetup);
    viewport.top = 0;
    viewport.left = 0;
    viewport.bottom = 600;
    viewport.right = 800;
    renderer->setWindowSize(OsmAnd::PointI(800, 600));
    renderer->setViewport(viewport);
    /*renderer->setTarget(OsmAnd::PointI(
        OsmAnd::Utilities::get31TileNumberX(34.0062),
        OsmAnd::Utilities::get31TileNumberY(44.4039)
        ));
    renderer->setZoom(18.0f);*/
    renderer->setZoom(1.5f);
    //renderer->setAzimuth(137.6f);
    renderer->setAzimuth(69.4f);
    renderer->setElevationAngle(35.0f);
    renderer->setFogColor(OsmAnd::FColorRGB(1.0f, 1.0f, 1.0f));
    offlineMapDataProvider.reset(new OsmAnd::OfflineMapDataProvider(obfsCollection, style, 1.0f));

    /// Amsterdam
    renderer->setTarget(OsmAnd::PointI(
        1102430866,
        704978668));
    //renderer->setZoom(10.0f);
    renderer->setZoom(4.0f);
    
    // Kiev
    /*renderer->setTarget(OsmAnd::PointI(
        1254096891,
        723769130));
    renderer->setZoom(10.0f);*/
    
    renderer->setAzimuth(0.0f);
    //renderer->setDisplayDensityFactor(2.0f);
    
    auto renderConfig = renderer->configuration;
    renderConfig.heixelsPerTileSide = 32;
    renderer->setConfiguration(renderConfig);
    
    renderer->initializeRendering();
    //////////////////////////////////////////////////////////////////////////

    glutMainLoop();

    {
        QMutexLocker scopedLocker(&glutWasInitializedFlagMutex);

        glutWasInitialized = false;
    }

    OsmAnd::ReleaseCore();
    return EXIT_SUCCESS;
}

void reshapeHandler(int newWidth, int newHeight)
{
    viewport.right = newWidth;
    viewport.bottom = newHeight;
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
    if(button == GLUT_LEFT_BUTTON)
    {
        if(state == GLUT_DOWN && !dragInitialized)
        {
            dragInitX = x;
            dragInitY = y;
            dragInitTarget = renderer->state.target31;

            dragInitialized = true;
        }
        else if(state == GLUT_UP && dragInitialized)
        {
            dragInitialized = false;
        }
    }
    else if(button == GLUT_RIGHT_BUTTON)
    {
        if(state == GLUT_DOWN)
        {
            OsmAnd::PointI64 clickedLocation;
            renderer->getLocationFromScreenPoint(OsmAnd::PointI(x, y), clickedLocation);
            lastClickedLocation31 = OsmAnd::Utilities::normalizeCoordinates(clickedLocation, OsmAnd::ZoomLevel31);
            
            auto delta = clickedLocation - OsmAnd::PointI64(renderer->state.target31.x, renderer->state.target31.y);

            animator->cancelAnimation();
            animator->animateTargetBy(delta, 1.0f);
            animator->resumeAnimation();
        }
    }
}

void mouseMotion(int x, int y)
{
    if(dragInitialized)
    {
        auto deltaX = x - dragInitX;
        auto deltaY = y - dragInitY;

        // Azimuth
        auto angle = qDegreesToRadians(renderer->state.azimuth);
        auto cosAngle = cosf(angle);
        auto sinAngle = sinf(angle);

        auto nx = deltaX * cosAngle - deltaY * sinAngle;
        auto ny = deltaX * sinAngle + deltaY * cosAngle;

        const auto tileSize31 = (1u << (31 - renderer->state.zoomBase));
        auto scale31 = static_cast<double>(tileSize31) / renderer->getScaledTileSizeOnScreen();

        OsmAnd::PointI newTarget;
        newTarget.x = dragInitTarget.x - static_cast<int32_t>(nx * scale31);
        newTarget.y = dragInitTarget.y - static_cast<int32_t>(ny * scale31);

        renderer->setTarget(newTarget);
    }
}

void mouseWheelHandler(int button, int dir, int x, int y)
{
    const auto modifiers = glutGetModifiers();
    const auto step = (modifiers & GLUT_ACTIVE_SHIFT) ? 0.1f : 0.01f;

    if(dir > 0)
    {
        renderer->setZoom(renderer->state.requestedZoom + step);
    }
    else
    {
        renderer->setZoom(renderer->state.requestedZoom - step);
    }
}

void keyboardHandler(unsigned char key, int x, int y)
{
    const auto modifiers = glutGetModifiers();
    const auto wasdZoom = static_cast<int>(renderer->state.requestedZoom);
    const auto wasdStep = (1 << (31 - wasdZoom));

    switch (key)
    {
    case '\x1B':
        {
            glutLeaveMainLoop();
        }
        break;
    case 'W':
    case 'w':
        {
            auto newTarget = renderer->state.target31;
            newTarget.y -= wasdStep / (key == 'w' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'S':
    case 's':
        {
            auto newTarget = renderer->state.target31;
            newTarget.y += wasdStep / (key == 's' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'A':
    case 'a':
        {
            auto newTarget = renderer->state.target31;
            newTarget.x -= wasdStep / (key == 'a' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'D':
    case 'd':
        {
            auto newTarget = renderer->state.target31;
            newTarget.x += wasdStep / (key == 'd' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'r':
        {
            renderer->setDistanceToFog(renderer->state.fogDistance + 1.0f);
        }
        break;
    case 'f':
        {
            renderer->setDistanceToFog(renderer->state.fogDistance - 1.0f);
        }
        break;
    case 'x':
        {
            renderWireframe = !renderWireframe;
            glutPostRedisplay();
        }
        break;
    case 'e':
        {
            if(renderer->state.elevationDataProvider)
            {
                renderer->resetElevationDataProvider();
            }
            else
            {
                if(wasHeightsDirSpecified)
                {
                    auto provider = new OsmAnd::HeightmapTileProvider(heightsDir, cacheDir.absoluteFilePath(OsmAnd::HeightmapTileProvider::defaultIndexFilename));
                    renderer->setElevationDataProvider(std::shared_ptr<OsmAnd::IMapElevationDataProvider>(provider));
                }
            }
        }
        break;
    case 't':
        {
            renderer->setFogDensity(renderer->state.fogDensity + 0.01f);
        }
        break;
    case 'g':
        {
            renderer->setFogDensity(renderer->state.fogDensity - 0.01f);
        }
        break;
    case 'u':
        {
            renderer->setFogOriginFactor(renderer->state.fogOriginFactor + 0.01f);
        }
        break;
    case 'j':
        {
            renderer->setFogOriginFactor(renderer->state.fogOriginFactor - 0.01f);
        }
        break;
    case 'i':
        {
            renderer->setFieldOfView(renderer->state.fieldOfView + 0.5f);
        }
        break;
    case 'k':
        {
            renderer->setFieldOfView(renderer->state.fieldOfView - 0.5f);
        }
        break;
    case 'o':
        {
            renderer->setElevationDataScaleFactor(renderer->state.elevationDataScaleFactor + 0.1f);
        }
        break;
    case 'l':
        {
            renderer->setElevationDataScaleFactor(renderer->state.elevationDataScaleFactor - 0.1f);
        }
        break;
    case 'v':
        {
            auto config = renderer->configuration;
            config.altasTexturesAllowed = !config.altasTexturesAllowed;
            renderer->setConfiguration(config);
        }
        break;
    case 'c':
        {
            auto config = renderer->configuration;
            config.limitTextureColorDepthBy16bits = !config.limitTextureColorDepthBy16bits;
            renderer->setConfiguration(config);
        }
        break;
    case 'b':
        {
            auto config = renderer->configuration;
            config.texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Normal;
            renderer->setConfiguration(config);
        }
        break;
    case 'n':
        {
            auto config = renderer->configuration;
            config.texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Good;
            renderer->setConfiguration(config);
        }
        break;
    case 'm':
        {
            auto config = renderer->configuration;
            config.texturesFilteringQuality = OsmAnd::TextureFilteringQuality::Best;
            renderer->setConfiguration(config);
        }
        break;
    /*case 'p':
        {
            auto config = renderer->configuration;
            config.paletteTexturesAllowed = !config.paletteTexturesAllowed;
            renderer->setConfiguration(config);
        }
        break;*/
    case 'z':
        {
            if(!renderer->state.symbolProviders.isEmpty())
                renderer->removeAllSymbolProviders();
            else
            {
                auto symbolProvider = new OsmAnd::OfflineMapSymbolProvider(offlineMapDataProvider);
                renderer->addSymbolProvider(std::shared_ptr<OsmAnd::IMapSymbolProvider>(symbolProvider));
            }
        }
        break;
    case 'q':
        {
            animator->cancelAnimation();
            animator->animateAzimuthBy(-renderer->state.azimuth, 1.0f);
            animator->resumeAnimation();
        }
        break;
    case '0':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::RasterMapLayerId::Overlay0 : OsmAnd::RasterMapLayerId::BaseLayer;
            activateProvider(layerId, 0);
        }
        break;
    case '1':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::RasterMapLayerId::Overlay0 : OsmAnd::RasterMapLayerId::BaseLayer;
            activateProvider(layerId, 1);
        }
        break;
    case '2':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::RasterMapLayerId::Overlay0 : OsmAnd::RasterMapLayerId::BaseLayer;
            activateProvider(layerId, 2);
        }
        break;
    case '3':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::RasterMapLayerId::Overlay0 : OsmAnd::RasterMapLayerId::BaseLayer;
            activateProvider(layerId, 3);
        }
        break;
    case '4':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::RasterMapLayerId::Overlay0 : OsmAnd::RasterMapLayerId::BaseLayer;
            activateProvider(layerId, 4);
        }
        break;
    case '5':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::RasterMapLayerId::Overlay0 : OsmAnd::RasterMapLayerId::BaseLayer;
            activateProvider(layerId, 5);
        }
        break;
    }
}

void specialHandler(int key, int x, int y)
{
    const auto modifiers = glutGetModifiers();
    const auto step = (modifiers & GLUT_ACTIVE_SHIFT) ? 1.0f : 0.1f;

    switch (key)
    {
    case GLUT_KEY_LEFT:
        {
            renderer->setAzimuth(renderer->state.azimuth + step);
        }
        break;
    case GLUT_KEY_RIGHT:
        {
            renderer->setAzimuth(renderer->state.azimuth - step);
        }
        break;
    case GLUT_KEY_UP:
        {
            renderer->setElevationAngle(renderer->state.elevationAngle + step);
        }
        break;
    case GLUT_KEY_DOWN:
        {
            renderer->setElevationAngle(renderer->state.elevationAngle - step);
        }
        break;
    }
}

void closeHandler(void)
{
    renderer->releaseRendering();
}

void activateProvider(OsmAnd::RasterMapLayerId layerId, int idx)
{
    if(idx == 0)
    {
        renderer->resetRasterLayerProvider(layerId);
    }
    else if(idx == 1)
    {
        auto tileProvider = OsmAnd::OnlineMapRasterTileProvider::createCycleMapProvider();
        renderer->setRasterLayerProvider(layerId, tileProvider);
    }
    else if(idx == 2)
    {
        auto tileProvider = OsmAnd::OnlineMapRasterTileProvider::createMapnikProvider();
        renderer->setRasterLayerProvider(layerId, tileProvider);
    }
    else if(idx == 3)
    {
        auto tileProvider = new OsmAnd::OfflineMapRasterTileProvider_Software(offlineMapDataProvider);
        renderer->setRasterLayerProvider(layerId, std::shared_ptr<OsmAnd::IMapBitmapTileProvider>(tileProvider));
    }
    else if(idx == 4)
    {
        auto tileProvider = new OsmAnd::OfflineMapRasterTileProvider_GPU(offlineMapDataProvider);
        renderer->setRasterLayerProvider(layerId, std::shared_ptr<OsmAnd::IMapBitmapTileProvider>(tileProvider));
    }
    else if(idx == 5)
    {
//        auto hillshadeTileProvider = new OsmAnd::HillshadeTileProvider();
//        renderer->setTileProvider(layerId, hillshadeTileProvider);
    }
}

void idleHandler(void)
{
    static std::chrono::time_point<std::chrono::high_resolution_clock> lastTimeStamp;
    static bool lastTimeStampInitialized = false;

    if(!lastTimeStampInitialized)
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

    if(renderer->prepareFrame())
        renderer->renderFrame();
    renderer->processRendering();
    verifyOpenGL();
    
    //////////////////////////////////////////////////////////////////////////
    if(!use43)
    {
        glMatrixMode(GL_PROJECTION);
        glLoadIdentity();
        gluOrtho2D(0, viewport.width(), 0, viewport.height());

        glMatrixMode(GL_MODELVIEW);
        glLoadIdentity();

        glPolygonMode(GL_FRONT_AND_BACK, GL_FILL);

        auto w = 390;
        auto h1 = 16*20;
        auto t = viewport.height();
        glColor4f(0.5f, 0.5f, 0.5f, 0.6f);
        glBegin(GL_QUADS);
            glVertex2f(0.0f,    t);
            glVertex2f(   w,    t);
            glVertex2f(   w, t-h1);
            glVertex2f(0.0f, t-h1);
        glEnd();
        verifyOpenGL();

        glColor3f(0.0f, 1.0f, 0.0f);
        glRasterPos2f(8, t - 16 * 1);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fov (keys i,k)         : %1").arg(renderer->state.fieldOfView)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 2);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fog distance (keys r,f): %1").arg(renderer->state.fogDistance)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 3);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("azimuth (arrows l,r)   : %1").arg(renderer->state.azimuth)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 4);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("pitch (arrows u,d)     : %1").arg(renderer->state.elevationAngle)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 5);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("target (keys w,a,s,d)  : %1 %2").arg(renderer->state.target31.x).arg(renderer->state.target31.y)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 6);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("zoom (mouse wheel)     : %1").arg(renderer->state.requestedZoom)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 7);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("zoom base              : %1").arg(renderer->state.zoomBase)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 8);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("zoom fraction          : %1").arg(renderer->state.zoomFraction)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 9);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("visible tiles          : %1").arg(renderer->getVisibleTilesCount())));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 10);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("wireframe (key x)      : %1").arg(renderWireframe)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 11);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("elevation data (key e) : %1").arg((bool)renderer->state.elevationDataProvider)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 12);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fog density (keys t,g) : %1").arg(renderer->state.fogDensity)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 13);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("fog origin F (keys u,j): %1").arg(renderer->state.fogOriginFactor)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 14);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("height scale (keys o,l): %1").arg(renderer->state.elevationDataScaleFactor)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 15);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("atlas textures (key v) : %1").arg(renderer->configuration.altasTexturesAllowed)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 16);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("16-bit textures (key c): %1").arg(renderer->configuration.limitTextureColorDepthBy16bits)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 17);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("tex-filtering (b,n,m)  : %1").arg(static_cast<int>(renderer->configuration.texturesFilteringQuality))));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 18);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("palette textures(key p): %1").arg(renderer->configuration.paletteTexturesAllowed)));
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 19);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("symbols (key z)        : %1").arg(!renderer->state.symbolProviders.isEmpty())));
        verifyOpenGL();

        glColor4f(0.5f, 0.5f, 0.5f, 0.6f);
        glBegin(GL_QUADS);
            glVertex2f(0.0f, 16*10);
            glVertex2f(   w, 16*10);
            glVertex2f(   w, 0.0f);
            glVertex2f(0.0f, 0.0f);
        glEnd();
        verifyOpenGL();

        glColor3f(0.0f, 1.0f, 0.0f);
        glRasterPos2f(8, 16 * 9);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Last clicked tile: (%1, %2)@%3").arg(lastClickedLocation31.x >> (31 - renderer->state.zoomBase)).arg(lastClickedLocation31.y >> (31 - renderer->state.zoomBase)).arg(renderer->state.zoomBase)));
        verifyOpenGL();

        glColor3f(0.0f, 1.0f, 0.0f);
        glRasterPos2f(8, 16 * 8);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Last clicked location: %1 %2").arg(lastClickedLocation31.x).arg(lastClickedLocation31.y)));
        verifyOpenGL();

        glColor3f(0.0f, 1.0f, 0.0f);
        glRasterPos2f(8, 16 * 7);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("Tile providers (holding alt controls overlay0):")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 6);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("0 - disable")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 5);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("1 - Mapnik")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 4);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("2 - CycleMap")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 3);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("3 - Offline maps (Software)")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 2);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("4 - Offline maps (GPU)")));
        verifyOpenGL();

        glRasterPos2f(8, 16 * 1);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)qPrintable(
            QString("5 - Hillshade")));
        verifyOpenGL();
    }
    
    glFlush();
    glutSwapBuffers();

    //glutPostRedisplay();
}

void verifyOpenGL()
{
    auto result = glGetError();
    if(result == GL_NO_ERROR)
        return;

    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Error, "Host OpenGL error 0x%08x : %s\n", result, gluErrorString(result));
}