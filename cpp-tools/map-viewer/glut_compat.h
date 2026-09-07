// A minimal GLUT-compatible surface implemented over GLFW, used on macOS only.
//
// freeglut on macOS is X11-only: Homebrew's libglut and XQuartz's own libGL are both Mesa, while
// OsmAndCore links Apple's OpenGL.framework. Two GL implementations end up in one process, GLUT
// makes a Mesa/GLX context current, core calls Apple CGL where nothing is current, and the viewer
// dies in glGetError(). GLFW creates an NSOpenGLContext against the same OpenGL.framework core
// uses, so there is a single GL stack and no X server involved.
//
// Only the subset of GLUT that map-viewer actually calls is provided, with GLUT semantics, so that
// main.cpp stays identical across platforms.

#ifndef _OSMAND_MAP_VIEWER_GLUT_COMPAT_H_
#define _OSMAND_MAP_VIEWER_GLUT_COMPAT_H_

#include <OpenGL/gl.h>
#include <OpenGL/glu.h>   // main.cpp's heads-up display uses gluOrtho2D

#define GLUT_RGBA                       0x0000
#define GLUT_DOUBLE                     0x0002
#define GLUT_DEPTH                      0x0010

#define GLUT_LEFT_BUTTON                0
#define GLUT_MIDDLE_BUTTON              1
#define GLUT_RIGHT_BUTTON               2
#define GLUT_DOWN                       0
#define GLUT_UP                         1

#define GLUT_ACTIVE_SHIFT               1
#define GLUT_ACTIVE_CTRL                2
#define GLUT_ACTIVE_ALT                 4

#define GLUT_KEY_F1                     1
#define GLUT_KEY_F2                     2
#define GLUT_KEY_F3                     3
#define GLUT_KEY_F4                     4
#define GLUT_KEY_F5                     5
#define GLUT_KEY_F6                     6
#define GLUT_KEY_LEFT                   100
#define GLUT_KEY_UP                     101
#define GLUT_KEY_RIGHT                  102
#define GLUT_KEY_DOWN                   103

#define GLUT_ACTION_ON_WINDOW_CLOSE     0x01F9
#define GLUT_ACTION_CONTINUE_EXECUTION  2

#define GLUT_CORE_PROFILE               0x0001
#define GLUT_COMPATIBILITY_PROFILE      0x0002
#define GLUT_DEBUG                      0x0001

// Only ever passed to glutBitmapString, which needs no real font handle.
#define GLUT_BITMAP_8_BY_13             ((void*)0x0008)

void glutInit(int* argcp, char** argv);
void glutInitWindowSize(int width, int height);
void glutInitDisplayMode(unsigned int mode);
void glutInitContextVersion(int major, int minor);
void glutInitContextProfile(int profile);
void glutInitContextFlags(int flags);
void glutSetOption(unsigned int option, int value);
int  glutCreateWindow(const char* title);

void glutReshapeFunc(void (*callback)(int, int));
void glutMouseFunc(void (*callback)(int, int, int, int));
void glutMotionFunc(void (*callback)(int, int));
void glutMouseWheelFunc(void (*callback)(int, int, int, int));
void glutKeyboardFunc(void (*callback)(unsigned char, int, int));
void glutSpecialFunc(void (*callback)(int, int, int));
void glutDisplayFunc(void (*callback)(void));
void glutIdleFunc(void (*callback)(void));
void glutCloseFunc(void (*callback)(void));

void glutMainLoop();
void glutLeaveMainLoop();
void glutPostRedisplay();
void glutSwapBuffers();
int  glutGetModifiers();

// Ratio between framebuffer pixels and window points (2 on a Retina display). The heads-up display
// in main.cpp is laid out in framebuffer pixels, so it has to scale by this to stay legible.
float glutCompatPixelScale();

// Bitmap text needs the fixed-function pipeline. Rendered when the context is a legacy one,
// collected for stdout otherwise - see glut_compat.cpp.
void glutBitmapString(void* font, const unsigned char* string);

#endif // !defined(_OSMAND_MAP_VIEWER_GLUT_COMPAT_H_)
