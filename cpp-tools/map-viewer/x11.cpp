#include <GL/freeglut.h>

#include <X11/Xlib.h>
#include <X11/keysym.h>

Display* xDisplay = nullptr;

void x11Init()
{
    xDisplay = XOpenDisplay(NULL);
}

void x11Release()
{
    XCloseDisplay(xDisplay);
}

void x11AlterModifiers(int& modifiers)
{
    char xKeymap[32];
    XQueryKeymap(xDisplay, xKeymap);

    const auto xkcCmdL = XKeysymToKeycode(xDisplay, XK_Meta_L);
    bool xkmCmdL = xkcCmdL > 0 && (((xKeymap[xkcCmdL >> 3]) >> (xkcCmdL % 8)) & 0x1);

    const auto xkcCmdR = XKeysymToKeycode(xDisplay, XK_Meta_R);
    bool xkmCmdR = xkcCmdR > 0 && (((xKeymap[xkcCmdR >> 3]) >> (xkcCmdR % 8)) & 0x1);

    // Map Command key as Alt on MacOS. See https://github.com/dcnieho/FreeGLUT/issues/94
    modifiers |= (xkmCmdL || xkmCmdR) ? GLUT_ACTIVE_ALT : 0;
}
