#include "glut_compat.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <map>
#include <utility>
#include <string>
#include <vector>

#include <GLFW/glfw3.h>

#include <CoreFoundation/CoreFoundation.h>
#include <CoreGraphics/CoreGraphics.h>
#include <CoreText/CoreText.h>

namespace
{
    GLFWwindow* window = nullptr;

    int initialWidth = 1024;
    int initialHeight = 760;
    int requestedMajor = 0;
    int requestedMinor = 0;
    int requestedProfile = 0;

    void (*reshapeCallback)(int, int) = nullptr;
    void (*mouseCallback)(int, int, int, int) = nullptr;
    void (*motionCallback)(int, int) = nullptr;
    void (*wheelCallback)(int, int, int, int) = nullptr;
    void (*keyboardCallback)(unsigned char, int, int) = nullptr;
    void (*specialCallback)(int, int, int) = nullptr;
    void (*displayCallback)(void) = nullptr;
    void (*idleCallback)(void) = nullptr;
    void (*closeCallback)(void) = nullptr;

    int modifiers = 0;
    int buttonsDown = 0;

    // Window coordinates are points, the framebuffer is pixels; on a Retina display they differ by
    // a factor of two. The renderer works in framebuffer pixels, so every coordinate handed to a
    // GLUT-style callback has to be scaled the same way.
    double pixelsPerPointX = 1.0;
    double pixelsPerPointY = 1.0;

    void updatePixelRatio()
    {
        int winW = 0, winH = 0, fbW = 0, fbH = 0;
        glfwGetWindowSize(window, &winW, &winH);
        glfwGetFramebufferSize(window, &fbW, &fbH);
        pixelsPerPointX = winW > 0 ? static_cast<double>(fbW) / winW : 1.0;
        pixelsPerPointY = winH > 0 ? static_cast<double>(fbH) / winH : 1.0;
    }

    void cursorInPixels(int& x, int& y)
    {
        double cx = 0.0, cy = 0.0;
        glfwGetCursorPos(window, &cx, &cy);
        x = static_cast<int>(cx * pixelsPerPointX);
        y = static_cast<int>(cy * pixelsPerPointY);
    }

    int translateModifiers(int glfwMods)
    {
        int result = 0;
        if (glfwMods & GLFW_MOD_SHIFT)
            result |= GLUT_ACTIVE_SHIFT;
        if (glfwMods & GLFW_MOD_CONTROL)
            result |= GLUT_ACTIVE_CTRL;
        // Command is mapped to Alt, matching what x11.cpp did for freeglut on macOS.
        if ((glfwMods & GLFW_MOD_ALT) || (glfwMods & GLFW_MOD_SUPER))
            result |= GLUT_ACTIVE_ALT;
        return result;
    }

    int translateSpecialKey(int key)
    {
        switch (key)
        {
            case GLFW_KEY_F1:    return GLUT_KEY_F1;
            case GLFW_KEY_F2:    return GLUT_KEY_F2;
            case GLFW_KEY_F3:    return GLUT_KEY_F3;
            case GLFW_KEY_F4:    return GLUT_KEY_F4;
            case GLFW_KEY_F5:    return GLUT_KEY_F5;
            case GLFW_KEY_F6:    return GLUT_KEY_F6;
            case GLFW_KEY_LEFT:  return GLUT_KEY_LEFT;
            case GLFW_KEY_UP:    return GLUT_KEY_UP;
            case GLFW_KEY_RIGHT: return GLUT_KEY_RIGHT;
            case GLFW_KEY_DOWN:  return GLUT_KEY_DOWN;
            default:             return -1;
        }
    }

    bool traceSizes = false;

    // Self-capture: OSMAND_SCREENSHOT=<path.ppm> [OSMAND_SCREENSHOT_AFTER=<seconds>] grabs the
    // framebuffer and quits. Lets the viewer be checked without any screen-recording permission.
    const char* screenshotPath = nullptr;
    double screenshotAfter = 3.0;

    // OSMAND_DRAG_TEST=<dx>,<dy> synthesizes a left-button drag through the very callbacks the
    // window system would drive, capturing a frame before and after. The map is then expected to
    // have moved by exactly (dx, dy) framebuffer pixels - measurable, unlike "looks right".
    int dragTestX = 0, dragTestY = 0;
    bool dragTestPending = false;



    const char* screenshotTarget = nullptr;

    void writeScreenshot()
    {
        int fbW = 0, fbH = 0;
        glfwGetFramebufferSize(window, &fbW, &fbH);
        if (fbW <= 0 || fbH <= 0)
            return;

        std::vector<unsigned char> pixels(static_cast<size_t>(fbW) * fbH * 3);
        glPixelStorei(GL_PACK_ALIGNMENT, 1);
        glReadPixels(0, 0, fbW, fbH, GL_RGB, GL_UNSIGNED_BYTE, pixels.data());

        const auto file = std::fopen(screenshotTarget, "wb");
        if (!file)
        {
            std::fprintf(stderr, "glut_compat: cannot write %s\n", screenshotTarget);
            return;
        }
        std::fprintf(file, "P6\n%d %d\n255\n", fbW, fbH);
        // glReadPixels returns bottom-up rows, PPM expects top-down.
        for (int row = fbH - 1; row >= 0; row--)
            std::fwrite(pixels.data() + static_cast<size_t>(row) * fbW * 3, 1, static_cast<size_t>(fbW) * 3, file);
        std::fclose(file);
        std::fprintf(stderr, "glut_compat: wrote %s (%dx%d)\n", screenshotTarget, fbW, fbH);
        std::fflush(stderr);
    }

    void runDragTest()
    {
        int fbW = 0, fbH = 0;
        glfwGetFramebufferSize(window, &fbW, &fbH);
        const auto cx = fbW / 2, cy = fbH / 2;

        std::string base = screenshotPath ? screenshotPath : "drag_test";
        static std::string beforePath;
        beforePath = base + ".before.ppm";
        screenshotTarget = beforePath.c_str();
        writeScreenshot();

        if (mouseCallback)
            mouseCallback(GLUT_LEFT_BUTTON, GLUT_DOWN, cx, cy);
        if (motionCallback)
            motionCallback(cx + dragTestX, cy + dragTestY);
        if (mouseCallback)
            mouseCallback(GLUT_LEFT_BUTTON, GLUT_UP, cx + dragTestX, cy + dragTestY);

        std::fprintf(stderr, "glut_compat: dragged from (%d,%d) by (%d,%d)\n",
            cx, cy, dragTestX, dragTestY);
        std::fflush(stderr);
    }

    void reportSize(const char* what, int width, int height)
    {
        if (!traceSizes)
            return;
        int winW = 0, winH = 0, fbW = 0, fbH = 0;
        glfwGetWindowSize(window, &winW, &winH);
        glfwGetFramebufferSize(window, &fbW, &fbH);
        std::fprintf(stderr, "glut_compat: %s %dx%d (window %dx%d, framebuffer %dx%d)\n",
            what, width, height, winW, winH, fbW, fbH);
        std::fflush(stderr);
    }

    void onFramebufferSize(GLFWwindow*, int width, int height)
    {
        updatePixelRatio();
        reportSize("reshape", width, height);
        if (reshapeCallback)
            reshapeCallback(width, height);
    }

    void onMouseButton(GLFWwindow*, int button, int action, int mods)
    {
        modifiers = translateModifiers(mods);

        int glutButton;
        switch (button)
        {
            case GLFW_MOUSE_BUTTON_LEFT:   glutButton = GLUT_LEFT_BUTTON; break;
            case GLFW_MOUSE_BUTTON_MIDDLE: glutButton = GLUT_MIDDLE_BUTTON; break;
            case GLFW_MOUSE_BUTTON_RIGHT:  glutButton = GLUT_RIGHT_BUTTON; break;
            default: return;
        }

        if (action == GLFW_PRESS)
            buttonsDown++;
        else if (buttonsDown > 0)
            buttonsDown--;

        int x = 0, y = 0;
        cursorInPixels(x, y);
        if (mouseCallback)
            mouseCallback(glutButton, action == GLFW_PRESS ? GLUT_DOWN : GLUT_UP, x, y);
    }

    void onCursorPos(GLFWwindow*, double, double)
    {
        // GLUT calls the motion callback only while a button is held.
        if (buttonsDown <= 0 || !motionCallback)
            return;

        int x = 0, y = 0;
        cursorInPixels(x, y);
        motionCallback(x, y);
    }

    void onScroll(GLFWwindow*, double, double yOffset)
    {
        if (!wheelCallback || yOffset == 0.0)
            return;

        int x = 0, y = 0;
        cursorInPixels(x, y);
        wheelCallback(0, yOffset > 0.0 ? 1 : -1, x, y);
    }

    void onKey(GLFWwindow*, int key, int, int action, int mods)
    {
        modifiers = translateModifiers(mods);
        if (action != GLFW_PRESS && action != GLFW_REPEAT)
            return;

        int x = 0, y = 0;
        cursorInPixels(x, y);

        const auto special = translateSpecialKey(key);
        if (special >= 0)
        {
            if (specialCallback)
                specialCallback(special, x, y);
            return;
        }

        // Keys that produce no character event but that GLUT still reports as ASCII.
        if (keyboardCallback)
        {
            switch (key)
            {
                case GLFW_KEY_ESCAPE:    keyboardCallback(27, x, y); break;
                case GLFW_KEY_BACKSPACE: keyboardCallback(8, x, y); break;
                case GLFW_KEY_ENTER:     keyboardCallback(13, x, y); break;
                case GLFW_KEY_TAB:       keyboardCallback(9, x, y); break;
                default: break;
            }
        }
    }

    void onChar(GLFWwindow*, unsigned int codepoint)
    {
        if (!keyboardCallback || codepoint > 127)
            return;

        int x = 0, y = 0;
        cursorInPixels(x, y);
        keyboardCallback(static_cast<unsigned char>(codepoint), x, y);
    }

    // GLUT draws heads-up text with a built-in bitmap font, which GLFW does not have. The glyphs
    // are rasterized with Core Text instead and blitted at the current raster position, so the
    // on-screen state and key hints in main.cpp keep working unchanged. Text is cached per string
    // (the panel redraws the same ~31 lines every frame) and rasterized white, with the caller's
    // glColor applied through the pixel transfer scales.
    struct TextBitmap
    {
        GLsizei width = 0;
        GLsizei height = 0;
        std::vector<unsigned char> rgba;
    };

    std::map<std::pair<std::string, int>, TextBitmap> textCache;

    const TextBitmap& rasterizeText(const std::string& text)
    {
        // The heads-up display is laid out in framebuffer pixels and macOS always hands out a
        // Retina-scaled framebuffer - GLFW_SCALE_FRAMEBUFFER and the older
        // GLFW_COCOA_RETINA_FRAMEBUFFER are both ignored there - so the font has to grow with it
        // or the text comes out half size.
        const auto fontSize = 11.0 * pixelsPerPointY;
        const auto cacheKey = std::make_pair(text, static_cast<int>(fontSize * 4.0));

        const auto existing = textCache.find(cacheKey);
        if (existing != textCache.end())
            return existing->second;

        TextBitmap bitmap;

        const auto cfText = CFStringCreateWithCString(nullptr, text.c_str(), kCFStringEncodingUTF8);
        const auto font = CTFontCreateWithName(CFSTR("Menlo"), fontSize, nullptr);
        const CGFloat components[] = { 1.0, 1.0, 1.0, 1.0 };
        const auto colorSpace = CGColorSpaceCreateDeviceRGB();
        const auto white = CGColorCreate(colorSpace, components);

        const void* keys[] = { kCTFontAttributeName, kCTForegroundColorAttributeName };
        const void* values[] = { font, white };
        const auto attributes = CFDictionaryCreate(nullptr, keys, values, 2,
            &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
        const auto attributed = CFAttributedStringCreate(nullptr, cfText, attributes);
        const auto line = CTLineCreateWithAttributedString(attributed);

        CGFloat ascent = 0.0, descent = 0.0, leading = 0.0;
        const auto advance = CTLineGetTypographicBounds(line, &ascent, &descent, &leading);

        bitmap.width = static_cast<GLsizei>(std::ceil(advance)) + 4;
        bitmap.height = static_cast<GLsizei>(std::ceil(ascent + descent)) + 4;
        bitmap.rgba.assign(static_cast<size_t>(bitmap.width) * bitmap.height * 4, 0);

        const auto context = CGBitmapContextCreate(bitmap.rgba.data(), bitmap.width, bitmap.height,
            8, static_cast<size_t>(bitmap.width) * 4, colorSpace,
            kCGImageAlphaPremultipliedLast | kCGBitmapByteOrder32Big);
        if (context)
        {
            // Draw in Core Graphics' own orientation (origin bottom-left, baseline above descent).
            CGContextSetTextPosition(context, 2.0, descent + 2.0);
            CTLineDraw(line, context);
            CGContextFlush(context);
            CGContextRelease(context);

            // A bitmap context stores row 0 at the top, glDrawPixels wants it at the bottom.
            const auto stride = static_cast<size_t>(bitmap.width) * 4;
            std::vector<unsigned char> row(stride);
            for (GLsizei y = 0; y < bitmap.height / 2; y++)
            {
                auto* const top = bitmap.rgba.data() + static_cast<size_t>(y) * stride;
                auto* const bot = bitmap.rgba.data() + static_cast<size_t>(bitmap.height - 1 - y) * stride;
                std::memcpy(row.data(), top, stride);
                std::memcpy(top, bot, stride);
                std::memcpy(bot, row.data(), stride);
            }
        }

        CFRelease(line);
        CFRelease(attributed);
        CFRelease(attributes);
        CGColorRelease(white);
        CGColorSpaceRelease(colorSpace);
        CFRelease(font);
        CFRelease(cfText);

        return textCache.emplace(cacheKey, std::move(bitmap)).first->second;
    }
}

void glutInit(int*, char**)
{
    traceSizes = getenv("OSMAND_TRACE_SIZES") != nullptr;
    screenshotPath = getenv("OSMAND_SCREENSHOT");
    if (const auto after = getenv("OSMAND_SCREENSHOT_AFTER"))
        screenshotAfter = atof(after);
    if (const auto drag = getenv("OSMAND_DRAG_TEST"))
    {
        dragTestPending = sscanf(drag, "%d,%d", &dragTestX, &dragTestY) == 2;
    }

    if (!glfwInit())
    {
        std::fprintf(stderr, "glut_compat: failed to initialize GLFW\n");
        std::exit(EXIT_FAILURE);
    }
}

void glutInitWindowSize(int width, int height)
{
    initialWidth = width;
    initialHeight = height;
}

void glutInitDisplayMode(unsigned int)
{
    // GLUT_RGBA | GLUT_DOUBLE | GLUT_DEPTH is what GLFW gives by default.
}

void glutInitContextVersion(int major, int minor)
{
    requestedMajor = major;
    requestedMinor = minor;
}

void glutInitContextProfile(int profile)
{
    requestedProfile = profile;
}

void glutInitContextFlags(int)
{
}

void glutSetOption(unsigned int, int)
{
}

int glutCreateWindow(const char* title)
{
    glfwWindowHint(GLFW_DEPTH_BITS, 24);
    glfwWindowHint(GLFW_DOUBLEBUFFER, GLFW_TRUE);

    // Without an explicit request macOS hands out a legacy 2.1 context, which is what the
    // fixed-function heads-up display in main.cpp needs. A core profile is only asked for when the
    // caller asked for one (-gl2 / -gl43).
    if (requestedMajor > 0 && requestedProfile == GLUT_CORE_PROFILE)
    {
        glfwWindowHint(GLFW_CONTEXT_VERSION_MAJOR, requestedMajor);
        glfwWindowHint(GLFW_CONTEXT_VERSION_MINOR, requestedMinor);
        glfwWindowHint(GLFW_OPENGL_PROFILE, GLFW_OPENGL_CORE_PROFILE);
        glfwWindowHint(GLFW_OPENGL_FORWARD_COMPAT, GLFW_TRUE);
    }

    window = glfwCreateWindow(initialWidth, initialHeight, title, nullptr, nullptr);
    if (!window)
    {
        std::fprintf(stderr, "glut_compat: failed to create a window/OpenGL context\n");
        glfwTerminate();
        std::exit(EXIT_FAILURE);
    }

    glfwMakeContextCurrent(window);
    glfwSwapInterval(1);
    updatePixelRatio();

    {
        int fbW = 0, fbH = 0;
        glfwGetFramebufferSize(window, &fbW, &fbH);
        reportSize("created", fbW, fbH);
    }

    glfwSetFramebufferSizeCallback(window, &onFramebufferSize);
    glfwSetMouseButtonCallback(window, &onMouseButton);
    glfwSetCursorPosCallback(window, &onCursorPos);
    glfwSetScrollCallback(window, &onScroll);
    glfwSetKeyCallback(window, &onKey);
    glfwSetCharCallback(window, &onChar);

    return 1;
}

void glutReshapeFunc(void (*callback)(int, int)) { reshapeCallback = callback; }
void glutMouseFunc(void (*callback)(int, int, int, int)) { mouseCallback = callback; }
void glutMotionFunc(void (*callback)(int, int)) { motionCallback = callback; }
void glutMouseWheelFunc(void (*callback)(int, int, int, int)) { wheelCallback = callback; }
void glutKeyboardFunc(void (*callback)(unsigned char, int, int)) { keyboardCallback = callback; }
void glutSpecialFunc(void (*callback)(int, int, int)) { specialCallback = callback; }
void glutDisplayFunc(void (*callback)(void)) { displayCallback = callback; }
void glutIdleFunc(void (*callback)(void)) { idleCallback = callback; }
void glutCloseFunc(void (*callback)(void)) { closeCallback = callback; }

void glutMainLoop()
{
    // GLUT delivers the first reshape before the first display.
    if (reshapeCallback)
    {
        int fbW = 0, fbH = 0;
        glfwGetFramebufferSize(window, &fbW, &fbH);
        reshapeCallback(fbW, fbH);
    }

    while (!glfwWindowShouldClose(window))
    {
        glfwPollEvents();

        if (idleCallback)
            idleCallback();

        if (displayCallback)
        {
            displayCallback();

            if (glfwGetTime() >= screenshotAfter)
            {
                if (dragTestPending)
                {
                    runDragTest();
                    dragTestPending = false;
                    screenshotAfter = glfwGetTime() + 3.0;   // let the new tiles settle
                }
                else if (screenshotPath)
                {
                    static std::string finalPath;
                    finalPath = std::string(screenshotPath) + (dragTestX || dragTestY ? ".after.ppm" : "");
                    screenshotTarget = finalPath.c_str();
                    writeScreenshot();
                    screenshotPath = nullptr;
                    glfwSetWindowShouldClose(window, GLFW_TRUE);
                }
            }
        }
    }

    if (closeCallback)
        closeCallback();

    glfwDestroyWindow(window);
    window = nullptr;
    glfwTerminate();
}

void glutLeaveMainLoop()
{
    if (window)
        glfwSetWindowShouldClose(window, GLFW_TRUE);
}

void glutPostRedisplay()
{
    // The loop redraws every iteration, so there is nothing to flag.
}

void glutSwapBuffers()
{
    if (window)
        glfwSwapBuffers(window);
}

int glutGetModifiers()
{
    return modifiers;
}

float glutCompatPixelScale()
{
    return static_cast<float>(pixelsPerPointY);
}

void glutBitmapString(void*, const unsigned char* string)
{
    if (!string || !*string)
        return;

    const auto& bitmap = rasterizeText(reinterpret_cast<const char*>(string));
    if (bitmap.rgba.empty())
        return;

    GLfloat color[4] = { 1.0f, 1.0f, 1.0f, 1.0f };
    glGetFloatv(GL_CURRENT_COLOR, color);

    // OsmAndCore's texture uploads leave the unpack state configured for their own buffers; a
    // stale GL_UNPACK_ROW_LENGTH in particular makes glDrawPixels read the glyph with the wrong
    // stride and smears it. Save the whole unpack state, normalise it, restore it afterwards.
    GLint alignment = 4, rowLength = 0, skipPixels = 0, skipRows = 0, swapBytes = 0, lsbFirst = 0;
    glGetIntegerv(GL_UNPACK_ALIGNMENT, &alignment);
    glGetIntegerv(GL_UNPACK_ROW_LENGTH, &rowLength);
    glGetIntegerv(GL_UNPACK_SKIP_PIXELS, &skipPixels);
    glGetIntegerv(GL_UNPACK_SKIP_ROWS, &skipRows);
    glGetIntegerv(GL_UNPACK_SWAP_BYTES, &swapBytes);
    glGetIntegerv(GL_UNPACK_LSB_FIRST, &lsbFirst);

    glPixelStorei(GL_UNPACK_ALIGNMENT, 1);
    glPixelStorei(GL_UNPACK_ROW_LENGTH, 0);
    glPixelStorei(GL_UNPACK_SKIP_PIXELS, 0);
    glPixelStorei(GL_UNPACK_SKIP_ROWS, 0);
    glPixelStorei(GL_UNPACK_SWAP_BYTES, GL_FALSE);
    glPixelStorei(GL_UNPACK_LSB_FIRST, GL_FALSE);
    glPixelZoom(1.0f, 1.0f);

    // The cached glyphs are white; tint them with whatever colour the caller selected.
    glPixelTransferf(GL_RED_SCALE, color[0]);
    glPixelTransferf(GL_GREEN_SCALE, color[1]);
    glPixelTransferf(GL_BLUE_SCALE, color[2]);

    glDrawPixels(bitmap.width, bitmap.height, GL_RGBA, GL_UNSIGNED_BYTE, bitmap.rgba.data());

    glPixelTransferf(GL_RED_SCALE, 1.0f);
    glPixelTransferf(GL_GREEN_SCALE, 1.0f);
    glPixelTransferf(GL_BLUE_SCALE, 1.0f);

    glPixelStorei(GL_UNPACK_ALIGNMENT, alignment);
    glPixelStorei(GL_UNPACK_ROW_LENGTH, rowLength);
    glPixelStorei(GL_UNPACK_SKIP_PIXELS, skipPixels);
    glPixelStorei(GL_UNPACK_SKIP_ROWS, skipRows);
    glPixelStorei(GL_UNPACK_SWAP_BYTES, swapBytes);
    glPixelStorei(GL_UNPACK_LSB_FIRST, lsbFirst);
}
