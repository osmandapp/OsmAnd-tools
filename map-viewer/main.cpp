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

#include <GL/glew.h>
#include <GL/freeglut.h>

#include <QString>
#include <QList>
#include <QFile>

#include <OsmAndCore.h>
#include <OsmAndCore/Common.h>
#include <OsmAndCore/Utilities.h>
#include <OsmAndCore/Logging.h>
#include <OsmAndCore/Map/Rasterizer.h>
#include <OsmAndCore/Map/RasterizerContext.h>
#include <OsmAndCore/Map/RasterizationStyles.h>
#include <OsmAndCore/Map/RasterizationStyleEvaluator.h>
#include <OsmAndCore/Map/MapDataCache.h>
#include <OsmAndCore/Map/IMapRenderer.h>
#include <OsmAndCore/Map/OnlineMapRasterTileProvider.h>
#include <OsmAndCore/Map/HillshadeTileProvider.h>
#include <OsmAndCore/Map/IMapElevationDataProvider.h>
#include <OsmAndCore/Map/HeightmapTileProvider.h>

OsmAnd::AreaI viewport;
std::shared_ptr<OsmAnd::IMapRenderer> renderer;

QDir cacheDir(QDir::current());
QDir heightsDir;
bool wasHeightsDirSpecified = false;
QList< std::shared_ptr<QFileInfo> > styleFiles;
QList< std::shared_ptr<QFileInfo> > obfFiles;
QString styleName;
bool wasObfRootSpecified = false;

bool use43 = false;

bool renderWireframe = false;
void reshapeHandler(int newWidth, int newHeight);
void mouseHandler(int button, int state, int x, int y);
void mouseMotion(int x, int y);
void mouseWheelHandler(int button, int dir, int x, int y);
void keyboardHandler(unsigned char key, int x, int y);
void specialHandler(int key, int x, int y);
void displayHandler(void);
void closeHandler(void);
void activateProvider(OsmAnd::MapTileLayerId layerId, int idx);
void verifyOpenGL();

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
            QDir obfRoot(arg.mid(strlen("-obfsDir=")));
            if(!obfRoot.exists())
            {
                std::cerr << "OBF directory does not exist" << std::endl;
                OsmAnd::ReleaseCore();
                return EXIT_FAILURE;
            }
            OsmAnd::Utilities::findFiles(obfRoot, QStringList() << "*.obf", obfFiles);
            wasObfRootSpecified = true;
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
    if(!wasObfRootSpecified)
        OsmAnd::Utilities::findFiles(QDir::current(), QStringList() << "*.obf", obfFiles);
    
    // Obtain and configure rasterization style context
    std::shared_ptr<OsmAnd::RasterizationStyle> style;
    if(!styleName.isEmpty())
    {
        OsmAnd::RasterizationStyles stylesCollection;
        for(auto itStyleFile = styleFiles.begin(); itStyleFile != styleFiles.end(); ++itStyleFile)
        {
            auto styleFile = *itStyleFile;

            if(!stylesCollection.registerStyle(*styleFile))
                std::cout << "Failed to parse metadata of '" << styleFile->fileName().toStdString() << "' or duplicate style" << std::endl;
        }
        if(!stylesCollection.obtainStyle(styleName, style))
        {
            std::cout << "Failed to resolve style '" << styleName.toStdString() << "'" << std::endl;
            OsmAnd::ReleaseCore();
            return EXIT_FAILURE;
        }
    }
    
    std::shared_ptr<OsmAnd::MapDataCache> mapDataCache(new OsmAnd::MapDataCache());
    for(auto itObf = obfFiles.begin(); itObf != obfFiles.end(); ++itObf)
    {
        auto obf = *itObf;
        std::shared_ptr<OsmAnd::ObfReader> obfReader(new OsmAnd::ObfReader(std::shared_ptr<QIODevice>(new QFile(obf->absoluteFilePath()))));

        mapDataCache->addSource(obfReader);
    }

#if defined(OSMAND_OPENGL_RENDERER_SUPPORTED)
    renderer = OsmAnd::createAtlasMapRenderer_OpenGL();
#endif
    if(!renderer)
    {
        std::cout << "No supported renderer" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }

    //////////////////////////////////////////////////////////////////////////

    assert(glutInit != nullptr);
    glutInit(&argc, argv);

    glutSetOption(GLUT_ACTION_ON_WINDOW_CLOSE, GLUT_ACTION_CONTINUE_EXECUTION);
    glutInitWindowSize(800, 600);
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
    glutCloseFunc(&closeHandler);
    verifyOpenGL();

    //////////////////////////////////////////////////////////////////////////
    activateProvider(OsmAnd::MapTileLayerId::RasterMap, 1);
    OsmAnd::MapRendererSetupOptions rendererSetup;
    rendererSetup.frameRequestCallback = []()
    {
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
    renderer->setElevationAngle(13.0f);
    renderer->setFogColor(1.0f, 1.0f, 1.0f);

    /// Amsterdam
    renderer->setTarget(OsmAnd::PointI(
        1102430866,
        704978668));
    renderer->setZoom(12.5f);
    /*
    // Kiev
    renderer->setTarget(OsmAnd::PointI(
        1254096891,
        723769130));
    renderer->setZoom(8.0f);
    */

    renderer->setAzimuth(0.0f);
    renderer->setElevationAngle(90.0f);
    //renderer->setDisplayDensityFactor(2.0f);
    
    renderer->initializeRendering();
    //////////////////////////////////////////////////////////////////////////

    glutMainLoop();

    
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

        int32_t tileSize31 = 1;
        if(renderer->state.zoomBase != OsmAnd::ZoomLevel31)
            tileSize31 = (1u << (31 - renderer->state.zoomBase)) - 1;
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
            if(renderer->state.tileProviders[OsmAnd::MapTileLayerId::ElevationData])
            {
                renderer->setTileProvider(OsmAnd::MapTileLayerId::ElevationData, std::shared_ptr<OsmAnd::IMapTileProvider>());
            }
            else
            {
                if(wasHeightsDirSpecified)
                {
                    auto provider = new OsmAnd::HeightmapTileProvider(heightsDir, cacheDir.absoluteFilePath(OsmAnd::HeightmapTileProvider::defaultIndexFilename));
                    //renderer->setHeightmapPatchesPerSide(provider->getMaxResolutionPatchesCount());
                    renderer->setTileProvider(OsmAnd::MapTileLayerId::ElevationData, std::shared_ptr<OsmAnd::IMapTileProvider>(provider));
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
            renderer->setHeightScaleFactor(renderer->state.heightScaleFactor + 0.1f);
        }
        break;
    case 'l':
        {
            renderer->setHeightScaleFactor(renderer->state.heightScaleFactor - 0.1f);
        }
        break;
    case 'v':
        {
            auto config = renderer->configuration;
            config.textureAtlasesAllowed = !config.textureAtlasesAllowed;
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
            config.texturesFilteringQuality = OsmAnd::MapRendererConfiguration::TextureFilteringQuality::Normal;
            renderer->setConfiguration(config);
        }
        break;
    case 'n':
        {
            auto config = renderer->configuration;
            config.texturesFilteringQuality = OsmAnd::MapRendererConfiguration::TextureFilteringQuality::Good;
            renderer->setConfiguration(config);
        }
        break;
    case 'm':
        {
            auto config = renderer->configuration;
            config.texturesFilteringQuality = OsmAnd::MapRendererConfiguration::TextureFilteringQuality::Best;
            renderer->setConfiguration(config);
        }
        break;
    case '0':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::MapTileLayerId::MapOverlay0 : OsmAnd::MapTileLayerId::RasterMap;
            activateProvider(layerId, 0);
        }
        break;
    case '1':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::MapTileLayerId::MapOverlay0 : OsmAnd::MapTileLayerId::RasterMap;
            activateProvider(layerId, 1);
        }
        break;
    case '2':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::MapTileLayerId::MapOverlay0 : OsmAnd::MapTileLayerId::RasterMap;
            activateProvider(layerId, 2);
        }
        break;
    case '3':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::MapTileLayerId::MapOverlay0 : OsmAnd::MapTileLayerId::RasterMap;
            activateProvider(layerId, 3);
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

void activateProvider(OsmAnd::MapTileLayerId layerId, int idx)
{
    if(idx == 0)
    {
        renderer->setTileProvider(layerId, std::shared_ptr<OsmAnd::IMapTileProvider>());
    }
    else if(idx == 1)
    {
        auto tileProvider = OsmAnd::OnlineMapRasterTileProvider::createCycleMapProvider();
        static_cast<OsmAnd::OnlineMapRasterTileProvider*>(tileProvider.get())->setLocalCachePath(QDir::current());
        renderer->setTileProvider(layerId, tileProvider);
    }
    else if(idx == 2)
    {
        auto tileProvider = OsmAnd::OnlineMapRasterTileProvider::createMapnikProvider();
        static_cast<OsmAnd::OnlineMapRasterTileProvider*>(tileProvider.get())->setLocalCachePath(QDir::current());
        renderer->setTileProvider(layerId, tileProvider);
    }
    else if(idx == 3)
    {
//        auto hillshadeTileProvider = new OsmAnd::HillshadeTileProvider();
//        renderer->setTileProvider(layerId, hillshadeTileProvider);
    }
}

void displayHandler()
{
    (void)glGetError();
    glPolygonMode(GL_FRONT_AND_BACK, renderWireframe ? GL_LINE : GL_FILL);
    verifyOpenGL();
    //////////////////////////////////////////////////////////////////////////

    //OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Debug, "-FS{-\n");
    glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT);
    verifyOpenGL();

    renderer->processRendering();
    renderer->renderFrame();
    renderer->postprocessRendering();
    verifyOpenGL();
    //OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Debug, "-}FS-\n");
    
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
        auto h1 = 16*18;
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
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fov (keys i,k)         : %1").arg(renderer->state.fieldOfView).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 2);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fog distance (keys r,f): %1").arg(renderer->state.fogDistance).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 3);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("azimuth (arrows l,r)   : %1").arg(renderer->state.azimuth).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 4);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("pitch (arrows u,d)     : %1").arg(renderer->state.elevationAngle).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 5);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("target (keys w,a,s,d)  : %1 %2").arg(renderer->state.target31.x).arg(renderer->state.target31.y).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 6);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("zoom (mouse wheel)     : %1").arg(renderer->state.requestedZoom).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 7);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("zoom base              : %1").arg(renderer->state.zoomBase).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 8);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("zoom fraction          : %1").arg(renderer->state.zoomFraction).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 9);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("visible tiles          : %1").arg(renderer->visibleTiles.size()).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 10);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("wireframe (key x)      : %1").arg(renderWireframe).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 11);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("elevation data (key e) : %1").arg((bool)renderer->state.tileProviders[OsmAnd::MapTileLayerId::ElevationData]).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 12);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fog density (keys t,g) : %1").arg(renderer->state.fogDensity).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 13);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fog origin F (keys u,j): %1").arg(renderer->state.fogOriginFactor).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 14);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("height scale (keys o,l): %1").arg(renderer->state.heightScaleFactor).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 15);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("atlas textures (key v) : %1").arg(renderer->configuration.textureAtlasesAllowed).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 16);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("16-bit textures (key c): %1").arg(renderer->configuration.limitTextureColorDepthBy16bits).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, t - 16 * 17);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("tex-filtering (b,n,m)  : %1").arg(static_cast<int>(renderer->configuration.texturesFilteringQuality)).toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, 16 * 6);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("Tile providers (holding alt controls overlay0):").toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, 16 * 5);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("0 - disable").toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, 16 * 4);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("1 - Mapnik").toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, 16 * 3);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("2 - CycleMap").toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, 16 * 2);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("3 - Vector maps").toStdString().c_str());
        verifyOpenGL();

        glRasterPos2f(8, 16 * 1);
        glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("4 - Hillshade").toStdString().c_str());
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