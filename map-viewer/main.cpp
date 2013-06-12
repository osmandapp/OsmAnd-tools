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
#include <GL/glut.h>

void keyboard(unsigned char key, int x, int y);
void display(void);

int main(int argc, char** argv)
{
    glutInit(&argc, argv);
    glutCreateWindow("OsmAnd Bird : 3D map render tool");
    glutKeyboardFunc(&keyboard);
    glutDisplayFunc(&display);
    glutMainLoop();

    return EXIT_SUCCESS;
}

void keyboard(unsigned char key, int x, int y)
{
    switch (key)
    {
    case '\x1B':
        exit(EXIT_SUCCESS);
        break;
    }
}

void display()
{
    glClear(GL_COLOR_BUFFER_BIT);

    glColor3f(1.0f, 0.0f, 0.0f);

    glBegin(GL_POLYGON);
    glVertex2f(-0.5f, -0.5f);
    glVertex2f( 0.5f, -0.5f);
    glVertex2f( 0.5f,  0.5f);
    glVertex2f(-0.5f,  0.5f);
    glEnd();

    glFlush();
}
