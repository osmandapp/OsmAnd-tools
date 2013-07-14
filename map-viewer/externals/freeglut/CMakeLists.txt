project(freeglut)

add_library(freeglut_static STATIC
	"config.h"
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
set(target_specific_public_includes "")
set(target_specific_public_libraries "")
if(CMAKE_TARGET_OS STREQUAL "linux" OR CMAKE_TARGET_OS STREQUAL "darwin")
	find_package(X11 REQUIRED)
	set(target_specific_public_includes ${target_specific_public_includes}
		${X11_INCLUDE_DIR}
	)
	set(target_specific_public_libraries ${target_specific_public_libraries}
		${X11_LIBRARIES}
	)
endif()

target_compile_definitions(freeglut_static
	PRIVATE
		-DHAVE_CONFIG_H
	PUBLIC
		-DFREEGLUT_STATIC
		${OPENGL_DEFINITIONS}
		${target_specific_public_definitions}
)
target_include_directories(freeglut_static
	PRIVATE
		"."
	PUBLIC
		${OPENGL_INCLUDE_DIRS}
		${target_specific_public_includes}
		"upstream.patched/include"
)
target_link_libraries(freeglut_static
	LINK_PUBLIC
		${OPENGL_LIBRARIES}
		${target_specific_public_libraries}
)
