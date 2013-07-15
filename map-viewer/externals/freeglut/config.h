#ifndef _OSMAND_FREEGLUT_H_
#define _OSMAND_FREEGLUT_H_

#if defined(_WIN32)
	/* Define to 1 if you have the <dlfcn.h> header file. */
	#undef HAVE_DLFCN_H

	/* Define to 1 if you don't have `vprintf' but do have `_doprnt.' */
	#undef HAVE_DOPRNT

	/* Define to 1 if you have the <errno.h> header file. */
	#undef HAVE_ERRNO_H

	/* Define to 1 if you have the <fcntl.h> header file. */
	#undef HAVE_FCNTL_H

	/* Define to 1 if you have the `gettimeofday' function. */
	#undef HAVE_GETTIMEOFDAY

	/* Define to 1 if you have the <GL/glu.h> header file. */
	#undef HAVE_GL_GLU_H

	/* Define to 1 if you have the <GL/glx.h> header file. */
	#undef HAVE_GL_GLX_H

	/* Define to 1 if you have the <GL/gl.h> header file. */
	#undef HAVE_GL_GL_H

	/* Define to 1 if you have the <inttypes.h> header file. */
	#undef HAVE_INTTYPES_H

	/* Define to 1 if you have the <libusbhid.h> header file. */
	#undef HAVE_LIBUSBHID_H

	/* Define to 1 if you have the `Xi' library (-lXi). */
	#undef HAVE_LIBXI

	/* Define to 1 if you have the `Xrandr' library (-lXrandr). */
	#undef HAVE_LIBXRANDR

	/* Define to 1 if you have the `Xxf86vm' library (-lXxf86vm). */
	#undef HAVE_LIBXXF86VM

	/* Define to 1 if you have the <limits.h> header file. */
	#undef HAVE_LIMITS_H

	/* Define to 1 if you have the <memory.h> header file. */
	#undef HAVE_MEMORY_H

	/* Define to 1 if you have the <stdint.h> header file. */
	#undef HAVE_STDINT_H

	/* Define to 1 if you have the <stdlib.h> header file. */
	#undef HAVE_STDLIB_H

	/* Define to 1 if you have the <strings.h> header file. */
	#undef HAVE_STRINGS_H

	/* Define to 1 if you have the <string.h> header file. */
	#undef HAVE_STRING_H

	/* Define to 1 if you have the <sys/ioctl.h> header file. */
	#undef HAVE_SYS_IOCTL_H

	/* Define to 1 if you have the <sys/param.h> header file. */
	#undef HAVE_SYS_PARAM_H

	/* Define to 1 if you have the <sys/stat.h> header file. */
	#undef HAVE_SYS_STAT_H

	/* Define to 1 if you have the <sys/time.h> header file. */
	#undef HAVE_SYS_TIME_H

	/* Define to 1 if you have the <sys/types.h> header file. */
	#undef HAVE_SYS_TYPES_H

	/* Define to 1 if you have the <unistd.h> header file. */
	#undef HAVE_UNISTD_H

	/* Define to 1 if you have the <usbhid.h> header file. */
	#undef HAVE_USBHID_H

	/* Define to 1 if you have the `vfprintf' function. */
	#undef HAVE_VFPRINTF

	/* Define to 1 if you have the `vprintf' function. */
	#undef HAVE_VPRINTF

	/* Define to 1 if you have the <X11/extensions/xf86vmode.h> header file. */
	#undef HAVE_X11_EXTENSIONS_XF86VMODE_H

	/* Define to 1 if you have the <X11/extensions/XInput2.h> header file. */
	#undef HAVE_X11_EXTENSIONS_XINPUT2_H

	/* Define to 1 if you have the <X11/extensions/XInput.h> header file. */
	#undef HAVE_X11_EXTENSIONS_XINPUT_H

	/* Define to 1 if you have the <X11/extensions/XI.h> header file. */
	#undef HAVE_X11_EXTENSIONS_XI_H

	/* Define to 1 if you have the <X11/extensions/Xrandr.h> header file. */
	#undef HAVE_X11_EXTENSIONS_XRANDR_H

	/* Define to the sub-directory in which libtool stores uninstalled libraries.
	   */
	#undef LT_OBJDIR

	/* Define to 1 if your C compiler doesn't accept -c and -o together. */
	#undef NO_MINUS_C_MINUS_O

	/* Name of package */
	#undef PACKAGE

	/* Define to the address where bug reports for this package should be sent. */
	#undef PACKAGE_BUGREPORT

	/* Define to the full name of this package. */
	#undef PACKAGE_NAME

	/* Define to the full name and version of this package. */
	#undef PACKAGE_STRING

	/* Define to the one symbol short name of this package. */
	#undef PACKAGE_TARNAME

	/* Define to the home page for this package. */
	#undef PACKAGE_URL

	/* Define to the version of this package. */
	#undef PACKAGE_VERSION

	/* Define to 1 if you have the ANSI C header files. */
	#undef STDC_HEADERS

	/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
	#undef TIME_WITH_SYS_TIME

	/* Version number of package */
	#undef VERSION

	/* Define to 1 if the X Window System is missing or not being used. */
	#undef X_DISPLAY_MISSING

	/* Define to 1 if you want to include debugging code. */
	#undef _DEBUG

	/* Define to empty if `const' does not conform to ANSI C. */
	#undef const
#elif defined(ANDROID) || defined(__ANDROID__) || defined(__APPLE__) || defined(__linux__)
	/* Define to 1 if you have the <dlfcn.h> header file. */
    #define HAVE_DLFCN_H 1

	/* Define to 1 if you don't have `vprintf' but do have `_doprnt.' */
	#undef HAVE_DOPRNT

	/* Define to 1 if you have the <errno.h> header file. */
	#define HAVE_ERRNO_H 1

	/* Define to 1 if you have the <fcntl.h> header file. */
	#define HAVE_FCNTL_H 1

	/* Define to 1 if you have the `gettimeofday' function. */
	#define HAVE_GETTIMEOFDAY 1

	/* Define to 1 if you have the <GL/glu.h> header file. */
	#define HAVE_GL_GLU_H 1

	/* Define to 1 if you have the <GL/glx.h> header file. */
	#define HAVE_GL_GLX_H 1

	/* Define to 1 if you have the <GL/gl.h> header file. */
	#define HAVE_GL_GL_H 1

	/* Define to 1 if you have the <inttypes.h> header file. */
	#define HAVE_INTTYPES_H 1

	/* Define to 1 if you have the <libusbhid.h> header file. */
	#undef HAVE_LIBUSBHID_H

	/* Define to 1 if you have the `Xi' library (-lXi). */
	#define HAVE_LIBXI 1

	/* Define to 1 if you have the `Xrandr' library (-lXrandr). */
	#define HAVE_LIBXRANDR 1

	/* Define to 1 if you have the `Xxf86vm' library (-lXxf86vm). */
	#define HAVE_LIBXXF86VM 1

	/* Define to 1 if you have the <limits.h> header file. */
	#define HAVE_LIMITS_H 1

	/* Define to 1 if you have the <memory.h> header file. */
	#define HAVE_MEMORY_H 1

	/* Define to 1 if you have the <stdint.h> header file. */
	#define HAVE_STDINT_H 1

	/* Define to 1 if you have the <stdlib.h> header file. */
	#define HAVE_STDLIB_H 1

	/* Define to 1 if you have the <strings.h> header file. */
	#define HAVE_STRINGS_H 1

	/* Define to 1 if you have the <string.h> header file. */
	#define HAVE_STRING_H 1

	/* Define to 1 if you have the <sys/ioctl.h> header file. */
	#define HAVE_SYS_IOCTL_H 1

	/* Define to 1 if you have the <sys/param.h> header file. */
	#define HAVE_SYS_PARAM_H 1

	/* Define to 1 if you have the <sys/stat.h> header file. */
	#define HAVE_SYS_STAT_H 1

	/* Define to 1 if you have the <sys/time.h> header file. */
	#define HAVE_SYS_TIME_H 1

	/* Define to 1 if you have the <sys/types.h> header file. */
	#define HAVE_SYS_TYPES_H 1

	/* Define to 1 if you have the <unistd.h> header file. */
	#define HAVE_UNISTD_H 1

	/* Define to 1 if you have the <usbhid.h> header file. */
	#undef HAVE_USBHID_H

	/* Define to 1 if you have the `vfprintf' function. */
	#define HAVE_VFPRINTF 1

	/* Define to 1 if you have the `vprintf' function. */
	#define HAVE_VPRINTF 1

	/* Define to 1 if you have the <X11/extensions/xf86vmode.h> header file. */
	#define HAVE_X11_EXTENSIONS_XF86VMODE_H 1

	/* Define to 1 if you have the <X11/extensions/XInput2.h> header file. */
	#define HAVE_X11_EXTENSIONS_XINPUT2_H 1

	/* Define to 1 if you have the <X11/extensions/XInput.h> header file. */
	#define HAVE_X11_EXTENSIONS_XINPUT_H 1

	/* Define to 1 if you have the <X11/extensions/XI.h> header file. */
	#define HAVE_X11_EXTENSIONS_XI_H 1

	/* Define to 1 if you have the <X11/extensions/Xrandr.h> header file. */
	#define HAVE_X11_EXTENSIONS_XRANDR_H 1

	/* Define to the sub-directory in which libtool stores uninstalled libraries.
	   */
	#undef LT_OBJDIR

	/* Define to 1 if your C compiler doesn't accept -c and -o together. */
	#undef NO_MINUS_C_MINUS_O

	/* Name of package */
	#undef PACKAGE

	/* Define to the address where bug reports for this package should be sent. */
	#undef PACKAGE_BUGREPORT

	/* Define to the full name of this package. */
	#undef PACKAGE_NAME

	/* Define to the full name and version of this package. */
	#undef PACKAGE_STRING

	/* Define to the one symbol short name of this package. */
	#undef PACKAGE_TARNAME

	/* Define to the home page for this package. */
	#undef PACKAGE_URL

	/* Define to the version of this package. */
	#undef PACKAGE_VERSION

	/* Define to 1 if you have the ANSI C header files. */
	#define STDC_HEADERS 1

	/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
	#undef TIME_WITH_SYS_TIME

	/* Version number of package */
	#undef VERSION

	/* Define to 1 if the X Window System is missing or not being used. */
	#undef X_DISPLAY_MISSING

	/* Define to empty if `const' does not conform to ANSI C. */
	#undef const
#endif

#endif // _OSMAND_FREEGLUT_H_
