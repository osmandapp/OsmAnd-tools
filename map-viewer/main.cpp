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
#include <OsmAndCommon.h>
#include <OsmAndUtilities.h>
#include <OsmAndLogging.h>
#include <Rasterizer.h>
#include <RasterizerContext.h>
#include <RasterizationStyles.h>
#include <RasterizationStyleEvaluator.h>
#include <MapDataCache.h>
#include <IMapRenderer.h>
#include <OnlineMapRasterTileProvider.h>
#include <IMapElevationDataProvider.h>
#include <OneDegreeMapElevationDataProvider_Flat.h>

OsmAnd::AreaI viewport;
std::shared_ptr<OsmAnd::IMapRenderer> renderer;

bool renderWireframe = false;
void reshapeHandler(int newWidth, int newHeight);
void mouseHandler(int button, int state, int x, int y);
void mouseWheelHandler(int button, int dir, int x, int y);
void keyboardHandler(unsigned char key, int x, int y);
void specialHandler(int key, int x, int y);
void displayHandler(void);
void activateProvider(OsmAnd::IMapRenderer::TileLayerId layerId, int idx);
void verifyOpenGL();

int main(int argc, char** argv)
{
    //////////////////////////////////////////////////////////////////////////
    OsmAnd::InitializeCore();

    QList< std::shared_ptr<QFile> > styleFiles;
    QList< std::shared_ptr<QFile> > obfFiles;
    QString styleName;
    bool wasObfRootSpecified = false;
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
    }
    if(!wasObfRootSpecified)
        OsmAnd::Utilities::findFiles(QDir::current(), QStringList() << "*.obf", obfFiles);
    if(obfFiles.isEmpty())
    {
        std::cerr << "No OBF files loaded" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }

    // Obtain and configure rasterization style context
    OsmAnd::RasterizationStyles stylesCollection;
    for(auto itStyleFile = styleFiles.begin(); itStyleFile != styleFiles.end(); ++itStyleFile)
    {
        auto styleFile = *itStyleFile;

        if(!stylesCollection.registerStyle(*styleFile))
            std::cout << "Failed to parse metadata of '" << styleFile->fileName().toStdString() << "' or duplicate style" << std::endl;
    }
    std::shared_ptr<OsmAnd::RasterizationStyle> style;
    if(!stylesCollection.obtainStyle(styleName, style))
    {
        std::cout << "Failed to resolve style '" << styleName.toStdString() << "'" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }
    
    std::shared_ptr<OsmAnd::MapDataCache> mapDataCache(new OsmAnd::MapDataCache());
    for(auto itObf = obfFiles.begin(); itObf != obfFiles.end(); ++itObf)
    {
        auto obf = *itObf;
        std::shared_ptr<OsmAnd::ObfReader> obfReader(new OsmAnd::ObfReader(obf));

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

    assert(glutInit);
    glutInit(&argc, argv);

    glutInitWindowSize(800, 600);
    glutInitDisplayMode(GLUT_DOUBLE | GLUT_RGBA | GLUT_DEPTH);
    glutInitContextVersion(3, 1);
    glutInitContextProfile(GLUT_CORE_PROFILE);
    assert(glutCreateWindow);
    glutCreateWindow((const char*)xT("OsmAnd Bird : 3D map render tool"));
    
    glutReshapeFunc(&reshapeHandler);
    glutMouseFunc(&mouseHandler);
    glutMouseWheelFunc(&mouseWheelHandler);
    glutKeyboardFunc(&keyboardHandler);
    glutSpecialFunc(&specialHandler);
    glutDisplayFunc(&displayHandler);
    verifyOpenGL();

    //////////////////////////////////////////////////////////////////////////
    activateProvider(OsmAnd::IMapRenderer::TileLayerId::RasterMap, 1);
    renderer->redrawRequestCallback = []()
    {
        glutPostRedisplay();
    };
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
    renderer->setZoom(1.75f);

    renderer->initializeRendering();
    //////////////////////////////////////////////////////////////////////////

    glutMainLoop();

    //////////////////////////////////////////////////////////////////////////
    renderer->releaseRendering();
    //////////////////////////////////////////////////////////////////////////

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

void mouseHandler(int button, int state, int x, int y)
{

}

void mouseWheelHandler(int button, int dir, int x, int y)
{
    const auto modifiers = glutGetModifiers();
    const auto step = (modifiers & GLUT_ACTIVE_SHIFT) ? 0.1f : 0.01f;

    if(dir > 0)
    {
        renderer->setZoom(renderer->configuration.requestedZoom + step);
    }
    else
    {
        renderer->setZoom(renderer->configuration.requestedZoom - step);
    }
}

void keyboardHandler(unsigned char key, int x, int y)
{
    const auto modifiers = glutGetModifiers();
    const auto wasdZoom = static_cast<int>(renderer->configuration.requestedZoom);
    const auto wasdStep = (1 << (31 - wasdZoom));

    switch (key)
    {
    case '\x1B':
        glutLeaveMainLoop();
        break;
    case 'W':
    case 'w':
        {
            auto newTarget = renderer->configuration.target31;
            newTarget.y -= wasdStep / (key == 'W' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'S':
    case 's':
        {
            auto newTarget = renderer->configuration.target31;
            newTarget.y += wasdStep / (key == 'S' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'A':
    case 'a':
        {
            auto newTarget = renderer->configuration.target31;
            newTarget.x -= wasdStep / (key == 'A' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'D':
    case 'd':
        {
            auto newTarget = renderer->configuration.target31;
            newTarget.x += wasdStep / (key == 'D' ? 50 : 10);
            renderer->setTarget(newTarget);
        }
        break;
    case 'r':
        {
            renderer->setDistanceToFog(renderer->configuration.fogDistance + 1.0f);
        }
        break;
    case 'f':
        {
            renderer->setDistanceToFog(renderer->configuration.fogDistance - 1.0f);
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
            if(renderer->configuration.tileProviders[OsmAnd::IMapRenderer::ElevationData])
            {
                renderer->setTileProvider(OsmAnd::IMapRenderer::ElevationData, std::shared_ptr<OsmAnd::IMapElevationDataProvider>());
            }
            else
            {
                auto provider = new OsmAnd::OneDegreeMapElevationDataProvider_Flat(renderer->configuration.heightmapPatchesPerSide * 2 + 1);
                renderer->setTileProvider(OsmAnd::IMapRenderer::TileLayerId::ElevationData, std::shared_ptr<OsmAnd::IMapElevationDataProvider>(provider));
            }
        }
        break;
    case 'z':
        {
            renderer->setTextureAtlasesUsagePermit(!renderer->configuration.textureAtlasesAllowed);
        }
        break;
    case 'c':
        {
            renderer->set16bitColorDepthLimit(!renderer->configuration.force16bitColorDepthLimit);
        }
        break;
    case 't':
        {
            renderer->setHeightmapPatchesPerSide(renderer->configuration.heightmapPatchesPerSide + 1);
        }
        break;
    case 'g':
        {
            renderer->setHeightmapPatchesPerSide(renderer->configuration.heightmapPatchesPerSide - 1);
        }
        break;
    case '0':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::IMapRenderer::TileLayerId::MapOverlay0 : OsmAnd::IMapRenderer::TileLayerId::RasterMap;
            activateProvider(layerId, 0);
        }
        break;
    case '1':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::IMapRenderer::TileLayerId::MapOverlay0 : OsmAnd::IMapRenderer::TileLayerId::RasterMap;
            activateProvider(layerId, 1);
        }
        break;
    case '2':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::IMapRenderer::TileLayerId::MapOverlay0 : OsmAnd::IMapRenderer::TileLayerId::RasterMap;
            activateProvider(layerId, 2);
        }
        break;
    case '3':
        {
            auto layerId = (modifiers & GLUT_ACTIVE_ALT) ? OsmAnd::IMapRenderer::TileLayerId::MapOverlay0 : OsmAnd::IMapRenderer::TileLayerId::RasterMap;
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
            renderer->setAzimuth(renderer->configuration.azimuth + step);
        }
        break;
    case GLUT_KEY_RIGHT:
        {
            renderer->setAzimuth(renderer->configuration.azimuth - step);
        }
        break;
    case GLUT_KEY_UP:
        {
            renderer->setElevationAngle(renderer->configuration.elevationAngle + step);
        }
        break;
    case GLUT_KEY_DOWN:
        {
            renderer->setElevationAngle(renderer->configuration.elevationAngle - step);
        }
        break;
    }
}

void activateProvider(OsmAnd::IMapRenderer::TileLayerId layerId, int idx)
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
}

void displayHandler()
{
    (void)glGetError();
    glPolygonMode( GL_FRONT_AND_BACK, renderWireframe ? GL_LINE : GL_FILL );
    //////////////////////////////////////////////////////////////////////////

    glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT);
    
    //OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Debug, "-FS{-\n");
    renderer->performRendering();
    //OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Debug, "-}FS-\n");
    
    //////////////////////////////////////////////////////////////////////////
    glMatrixMode(GL_PROJECTION);
    glLoadIdentity();
    gluOrtho2D(0, viewport.width(), 0, viewport.height());

    glMatrixMode(GL_MODELVIEW);
    glLoadIdentity();

    glColor3f(1.0f, 0.0f, 0.0f);
    glRasterPos2f(8, viewport.height() - 16 * 1);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fov                   : %1").arg(renderer->configuration.fieldOfView).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 2);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fog (keys r,f)        : %1").arg(renderer->configuration.fogDistance).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 3);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("azimuth (arrows l,r)  : %1").arg(renderer->configuration.azimuth).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 4);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("pitch (arrows u,d)     : %1").arg(renderer->configuration.elevationAngle).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 5);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("target (keys w,a,s,d)  : %1 %2").arg(renderer->configuration.target31.x).arg(renderer->configuration.target31.y).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 6);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("zoom (mouse wheel)     : %1").arg(renderer->configuration.requestedZoom).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 7);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("zoom base              : %1").arg(renderer->configuration.zoomBase).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 8);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("zoom fraction          : %1").arg(renderer->configuration.zoomFraction).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 9);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("visible tiles          : %1").arg(renderer->visibleTiles.size()).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 10);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("wireframe (key x)      : %1").arg(renderWireframe).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 11);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("elevation data (key e) : %1").arg((bool)renderer->configuration.tileProviders[OsmAnd::IMapRenderer::ElevationData]).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 12);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("use atlases (key z)    : %1").arg(renderer->configuration.textureAtlasesAllowed).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 13);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("16bit limit (key c)    : %1").arg(renderer->configuration.force16bitColorDepthLimit).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, viewport.height() - 16 * 14);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("DEM-patches# (keys t,g): %1").arg(renderer->configuration.heightmapPatchesPerSide).toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, 16 * 5);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("Tile providers (holding alt controls overlay0):").toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, 16 * 4);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("0 - disable").toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, 16 * 3);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("1 - Mapnik").toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, 16 * 2);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("2 - CycleMap").toStdString().c_str());
    verifyOpenGL();

    glRasterPos2f(8, 16 * 1);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("3 - Vector maps").toStdString().c_str());
    verifyOpenGL();
    
    glFlush();
    glutSwapBuffers();
}

void verifyOpenGL()
{
    auto result = glGetError();
    if(result == GL_NO_ERROR)
        return;

    OsmAnd::LogPrintf(OsmAnd::LogSeverityLevel::Error, "Host OpenGL error 0x%08x : %s\n", result, gluErrorString(result));
}