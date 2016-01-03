project(freeglut)

prepare_upstream()

save_and_set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${CMAKE_C_FLAGS}")

include(CheckIncludeFile)
include(CheckIncludeFiles)
include(CheckFunctionExists)
include(CheckSymbolExists)
include(TestBigEndian)

check_include_file("dlfcn.h" HAVE_DLFCN_H)
check_function_exists("_doprnt" HAVE_DOPRNT)
check_include_file("errno.h" HAVE_ERRNO_H)
check_include_file("fcntl.h" HAVE_FCNTL_H)
check_function_exists("gettimeofday" HAVE_GETTIMEOFDAY)
check_include_file("GL/glu.h" HAVE_GL_GLU_H)
check_include_file("GL/glx.h" HAVE_GL_GLX_H)
check_include_file("GL/gl.h" HAVE_GL_GL_H)
check_include_file("inttypes.h" HAVE_INTTYPES_H)
check_include_file("libusbhid.h" HAVE_LIBUSBHID_H)
unset(HAVE_LIBXI)
unset(HAVE_LIBXRANDR)
unset(HAVE_LIBXXF86VM)
check_include_file("limits.h" HAVE_LIMITS_H)
check_include_file("memory.h" HAVE_MEMORY_H)
check_include_file("stdint.h" HAVE_STDINT_H)
check_include_file("stdlib.h" HAVE_STDLIB_H)
check_include_file("strings.h" HAVE_STRINGS_H)
check_include_file("string.h" HAVE_STRING_H)
check_include_file("sys/ioctl.h" HAVE_SYS_IOCTL_H)
check_include_file("sys/param.h" HAVE_SYS_PARAM_H)
check_include_file("sys/stat.h" HAVE_SYS_STAT_H)
check_include_file("sys/time.h" HAVE_SYS_TIME_H)
check_include_file("sys/types.h" HAVE_SYS_TYPES_H)
check_include_file("unistd.h" HAVE_UNISTD_H)
check_include_file("usbhid.h" HAVE_USBHID_H)
if (CMAKE_TARGET_OS STREQUAL "windows")
	unset(HAVE_VFPRINTF)
else()
	check_function_exists("vfprintf" HAVE_VFPRINTF)
endif()
check_function_exists("vprintf" HAVE_VPRINTF)
check_include_files("X11/Xlib.h;X11/extensions/xf86vmode.h" HAVE_X11_EXTENSIONS_XF86VMODE_H)
check_include_files("X11/Xlib.h;X11/extensions/XInput2.h" HAVE_X11_EXTENSIONS_XINPUT2_H)
check_include_files("X11/Xlib.h;X11/extensions/XInput.h" HAVE_X11_EXTENSIONS_XINPUT_H)
check_include_files("X11/Xlib.h;X11/extensions/XI.h" HAVE_X11_EXTENSIONS_XI_H)
check_include_files("X11/Xlib.h;X11/extensions/Xrandr.h" HAVE_X11_EXTENSIONS_XRANDR_H)
unset(LT_OBJDIR)
unset(NO_MINUS_C_MINUS_O)
unset(PACKAGE)
unset(PACKAGE_BUGREPORT)
unset(PACKAGE_NAME)
unset(PACKAGE_STRING)
unset(PACKAGE_TARNAME)
unset(PACKAGE_URL)
unset(PACKAGE_VERSION)
check_include_files("stdlib.h;stdarg.h;string.h;float.h" STDC_HEADERS)
unset(TIME_WITH_SYS_TIME)
unset(VERSION)
unset(X_DISPLAY_MISSING)
unset(_DEBUG)
unset(const)

restore(CMAKE_REQUIRED_FLAGS)

configure_file("config.h.cmake" "${CMAKE_CURRENT_BINARY_DIR}/config.h")

add_library(freeglut_static STATIC
	"upstream.patched/include/GL/freeglut.h"
	"upstream.patched/include/GL/freeglut_ext.h"
	"upstream.patched/include/GL/freeglut_std.h"
	"upstream.patched/include/GL/glut.h"
	"upstream.patched/src/freeglut_callbacks.c"
	"upstream.patched/src/freeglut_cursor.c"
	"upstream.patched/src/freeglut_display.c"
	"upstream.patched/src/freeglut_ext.c"
	"upstream.patched/src/freeglut_font.c"
	"upstream.patched/src/freeglut_glutfont_definitions.c"
	"upstream.patched/src/freeglut_font_data.c"
	"upstream.patched/src/freeglut_stroke_roman.c"
	"upstream.patched/src/freeglut_stroke_mono_roman.c"
	"upstream.patched/src/freeglut_gamemode.c"
	"upstream.patched/src/freeglut_geometry.c"
	"upstream.patched/src/freeglut_init.c"
	"upstream.patched/src/freeglut_input_devices.c"
	"upstream.patched/src/freeglut_spaceball.c"
	"upstream.patched/src/freeglut_joystick.c"
	"upstream.patched/src/freeglut_main.c"
	"upstream.patched/src/freeglut_menu.c"
	"upstream.patched/src/freeglut_misc.c"
	"upstream.patched/src/freeglut_overlay.c"
	"upstream.patched/src/freeglut_state.c"
	"upstream.patched/src/freeglut_structure.c"
	"upstream.patched/src/freeglut_teapot.c"
	"upstream.patched/src/freeglut_videoresize.c"
	"upstream.patched/src/freeglut_window.c"
	"upstream.patched/src/freeglut_xinput.c"
)

find_package(OpenGL REQUIRED)

set(target_specific_public_definitions "")
set(target_specific_private_definitions "")
set(target_specific_public_includes "")
set(target_specific_public_libraries "")
if (CMAKE_TARGET_OS STREQUAL "linux" OR CMAKE_TARGET_OS STREQUAL "macosx")
	find_package(X11 REQUIRED)
	set(target_specific_public_includes ${target_specific_public_includes}
		${X11_INCLUDE_DIR}
	)
	find_library(X11_Xxf86vm "Xxf86vm" PATHS ${X11_LIBRARY_DIR})
	find_library(X11_Xi "Xi" PATHS ${X11_LIBRARY_DIR})
	find_library(X11_Xrandr "Xrandr" PATHS ${X11_LIBRARY_DIR})
	set(target_specific_public_libraries ${target_specific_public_libraries}
		${X11_LIBRARIES}
		${X11_Xxf86vm}
		${X11_Xi}
		${X11_Xrandr}
	)
endif()

if (CMAKE_TARGET_OS STREQUAL "macosx")
	find_package(GLUT REQUIRED)
	set(target_specific_public_includes ${target_specific_public_includes}
		${GLUT_INCLUDE_DIR}
	)
	set(target_specific_public_libraries ${target_specific_public_libraries}
		${GLUT_LIBRARIES}
	)

	find_library(X11_GL "GL" PATHS ${X11_LIBRARY_DIR})
	set(target_specific_public_libraries ${target_specific_public_libraries}
		${X11_GL}
	)
elseif (CMAKE_TARGET_OS STREQUAL "windows")
	set(target_specific_public_libraries ${target_specific_public_libraries}
		winmm
	)

	if (CMAKE_COMPILER_FAMILY STREQUAL "msvc")
		set(target_specific_private_definitions ${target_specific_private_definitions}
			-D_CRT_SECURE_NO_WARNINGS
		)
	endif()
endif()

target_compile_definitions(freeglut_static
	PRIVATE
		-DHAVE_CONFIG_H
		${target_specific_private_definitions}
	PUBLIC
		-DFREEGLUT_STATIC
		${OPENGL_DEFINITIONS}
		${target_specific_public_definitions}
)
target_include_directories(freeglut_static
	PRIVATE
		"${CMAKE_CURRENT_BINARY_DIR}"
	PUBLIC
		"upstream.patched/include"
		${OPENGL_INCLUDE_DIRS}
		${target_specific_public_includes}
)
target_link_libraries(freeglut_static
	LINK_PUBLIC
		${OPENGL_LIBRARIES}
		${target_specific_public_libraries}
)
