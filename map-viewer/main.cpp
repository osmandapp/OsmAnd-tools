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
#include <Rasterizer.h>
#include <RasterizerContext.h>
#include <RasterizationStyles.h>
#include <RasterizationStyleEvaluator.h>
#include <MapDataCache.h>
#include <IRenderer.h>
#include <OnlineMapRasterTileProvider.h>

OsmAnd::AreaI viewport;
std::shared_ptr<OsmAnd::IRenderer> renderer;

void reshapeHandler(int newWidth, int newHeight);
void mouseHandler(int button, int state, int x, int y);
void mouseWheelHandler(int button, int dir, int x, int y);
void keyboardHandler(unsigned char key, int x, int y);
void specialHandler(int key, int x, int y);
void displayHandler(void);

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
    renderer = OsmAnd::createRenderer_OpenGL();
#endif
    if(!renderer)
    {
        std::cout << "No supported renderer" << std::endl;
        OsmAnd::ReleaseCore();
        return EXIT_FAILURE;
    }

    //////////////////////////////////////////////////////////////////////////

    glutInit(&argc, argv);

    glutInitWindowSize(800, 600);
    glutInitDisplayMode(GLUT_DOUBLE | GLUT_RGBA | GLUT_DEPTH);
    glutInitContextVersion(3, 0);
    glutInitContextProfile(GLUT_CORE_PROFILE);
    glutCreateWindow((const char*)xT("OsmAnd Bird : 3D map render tool"));
    
    glutReshapeFunc(&reshapeHandler);
    glutMouseFunc(&mouseHandler);
    glutMouseWheelFunc(&mouseWheelHandler);
    glutKeyboardFunc(&keyboardHandler);
    glutSpecialFunc(&specialHandler);
    glutDisplayFunc(&displayHandler);

    //////////////////////////////////////////////////////////////////////////
    auto tileProvider = OsmAnd::OnlineMapRasterTileProvider::createCycleMapProvider();
    static_cast<OsmAnd::OnlineMapRasterTileProvider*>(tileProvider.get())->setLocalCachePath(QDir::current());
    renderer->setTileProvider(tileProvider);
    renderer->redrawRequestCallback = []()
    {
        glutPostRedisplay();
    };
    viewport.top = 0;
    viewport.left = 0;
    viewport.bottom = 600;
    viewport.right = 800;
    renderer->updateViewport(OsmAnd::PointI(800, 600), viewport, renderer->configuration.fieldOfView, renderer->configuration.fogDistance);
    renderer->updateMap(renderer->configuration.target31, 0);
    
    glClearColor(0.0f, 0.0f, 0.0f, 0.0f);
    glShadeModel(GL_SMOOTH);
    glClearDepth(1.0f);
    glEnable(GL_DEPTH_TEST);
    glDepthFunc(GL_LEQUAL);
    glDisable(GL_CULL_FACE);
    glHint(GL_PERSPECTIVE_CORRECTION_HINT, GL_NICEST);
    //////////////////////////////////////////////////////////////////////////

    glutMainLoop();

    //////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    OsmAnd::ReleaseCore();
    return EXIT_SUCCESS;
}

void reshapeHandler(int newWidth, int newHeight)
{
    viewport.right = newWidth;
    viewport.bottom = newHeight;
    renderer->updateViewport(OsmAnd::PointI(newWidth, newHeight), viewport, renderer->configuration.fieldOfView, renderer->configuration.fogDistance);

    glViewport(0, 0, newWidth, newHeight);
}

void mouseHandler(int button, int state, int x, int y)
{

}

void mouseWheelHandler(int button, int dir, int x, int y)
{
    if(dir > 0)
    {
        if(renderer->configuration.distanceFromTarget > 0)
        {
            renderer->updateCamera(renderer->configuration.distanceFromTarget - 1, renderer->configuration.azimuth, renderer->configuration.elevationAngle);
        }
    }
    else
    {
        if(renderer->configuration.distanceFromTarget < 100000)
        {
            renderer->updateCamera(renderer->configuration.distanceFromTarget + 1, renderer->configuration.azimuth, renderer->configuration.elevationAngle);
        }
    }
}

void keyboardHandler(unsigned char key, int x, int y)
{
    switch (key)
    {
    case '\x1B':
        glutLeaveMainLoop();
        break;
    case 'q':
        {
            if(renderer->configuration.zoom > 0)
            {
                renderer->updateMap(renderer->configuration.target31, renderer->configuration.zoom - 1);
            }
        }
        break;
    case 'e':
        {
            if(renderer->configuration.zoom < 31)
            {
                renderer->updateMap(renderer->configuration.target31, renderer->configuration.zoom + 1);
            }
        }
        break;
    case 'w':
        {
            if(renderer->configuration.target31.y >= (1 << renderer->configuration.zoom) / 100)
            {
                auto newTarget = renderer->configuration.target31;
                newTarget.y = qMax(0, newTarget.y - (1 << renderer->configuration.zoom) / 100);

                renderer->updateMap(newTarget, renderer->configuration.zoom);
            }
        }
        break;
    case 's':
        {
            if(renderer->configuration.target31.y <= std::numeric_limits<int32_t>::max() - (1 << renderer->configuration.zoom) / 100)
            {
                auto newTarget = renderer->configuration.target31;
                newTarget.y = qMin(std::numeric_limits<int32_t>::max(), newTarget.y + (1 << renderer->configuration.zoom) / 100);

                renderer->updateMap(newTarget, renderer->configuration.zoom);
            }
        }
        break;
    case 'a':
        {
            if(renderer->configuration.target31.x >= (1 << renderer->configuration.zoom) / 100)
            {
                auto newTarget = renderer->configuration.target31;
                newTarget.x = qMax(0, newTarget.x - (1 << renderer->configuration.zoom) / 100);

                renderer->updateMap(newTarget, renderer->configuration.zoom);
            }
        }
        break;
    case 'd':
        {
            if(renderer->configuration.target31.x <= std::numeric_limits<int32_t>::max() - (1 << renderer->configuration.zoom) / 100)
            {
                auto newTarget = renderer->configuration.target31;
                newTarget.x = qMin(std::numeric_limits<int32_t>::max(), newTarget.x + (1 << renderer->configuration.zoom) / 100);

                renderer->updateMap(newTarget, renderer->configuration.zoom);
            }
        }
        break;
    case 'r':
        {
            if(renderer->configuration.fogDistance < 15000.0f)
            {
                renderer->updateViewport(renderer->configuration.windowSize, renderer->configuration.viewport, renderer->configuration.fieldOfView, renderer->configuration.fogDistance + 1.0f);
            }
        }
        break;
    case 'f':
        {
            if(renderer->configuration.fogDistance >= 1.0f)
            {
                renderer->updateViewport(renderer->configuration.windowSize, renderer->configuration.viewport, renderer->configuration.fieldOfView, renderer->configuration.fogDistance - 1.0f);
            }
        }
        break;
    }
}

void specialHandler(int key, int x, int y)
{
    switch (key)
    {
    case GLUT_KEY_LEFT:
        {
            if(renderer->configuration.azimuth <= 360.0f - 1.0f)
            {
                renderer->updateCamera(renderer->configuration.distanceFromTarget, renderer->configuration.azimuth + 1.0f, renderer->configuration.elevationAngle);
            }
        }
        break;
    case GLUT_KEY_RIGHT:
        {
            if(renderer->configuration.azimuth >= 1.0f)
            {
                renderer->updateCamera(renderer->configuration.distanceFromTarget, renderer->configuration.azimuth - 1.0f, renderer->configuration.elevationAngle);
            }
        }
        break;
    case GLUT_KEY_UP:
        {
            if(renderer->configuration.elevationAngle <= 90.0f - 1.0f)
            {
                renderer->updateCamera(renderer->configuration.distanceFromTarget, renderer->configuration.azimuth, renderer->configuration.elevationAngle + 1.0f);
            }
        }
        break;
    case GLUT_KEY_DOWN:
        {
            if(renderer->configuration.elevationAngle >= 1.0f)
            {
                renderer->updateCamera(renderer->configuration.distanceFromTarget, renderer->configuration.azimuth, renderer->configuration.elevationAngle - 1.0f);
            }
        }
        break;
    }
}

void displayHandler()
{
    glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT);
    
    if(!renderer->isRenderingInitialized)
        renderer->initializeRendering();
    renderer->performRendering();
    
    //////////////////////////////////////////////////////////////////////////
    
    glMatrixMode(GL_PROJECTION);
    glLoadIdentity();
    gluOrtho2D(0, viewport.width(), 0, viewport.height());

    glMatrixMode(GL_MODELVIEW);
    glLoadIdentity();

    glColor3f(1.0f, 1.0f, 1.0f);
    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 1);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fov                   : %1").arg(renderer->configuration.fieldOfView).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 2);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("fog (keys r,f)        : %1").arg(renderer->configuration.fogDistance).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 3);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("distance (mouse wheel): %1").arg(renderer->configuration.distanceFromTarget).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 4);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("azimuth (arrows l,r)  : %1").arg(renderer->configuration.azimuth).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 5);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("pitch (arrows u,d)    : %1").arg(renderer->configuration.elevationAngle).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 6);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("target (keys w,a,s,d) : %1 %2").arg(renderer->configuration.target31.x).arg(renderer->configuration.target31.y).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 7);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("zoom (keys q,e)       : %1").arg(renderer->configuration.zoom).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 8);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("visible tiles         : %1").arg(renderer->visibleTiles.size()).toStdString().c_str());
    glPopMatrix();

    glPushMatrix();
    glRasterPos2f(8, viewport.height() - 16 * 9);
    glutBitmapString(GLUT_BITMAP_8_BY_13, (const unsigned char*)QString("cached tile textures  : %1").arg(renderer->getCachedTilesCount()).toStdString().c_str());
    glPopMatrix();

    glFlush();
    glutSwapBuffers();
}
