/**
 * hdr_time.h
 * Written by Michael Barker and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 */

#ifndef HDR_TIME_H__
#define HDR_TIME_H__

#include <time.h>

#if defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__)

typedef struct hdr_timespec
{
    long tv_sec;
    long tv_nsec;
} hdr_timespec;

#else

typedef struct timespec hdr_timespec;

#endif

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_MSC_VER)
void hdr_gettime(hdr_timespec* t);
#else
void hdr_gettime(hdr_timespec* t);
#endif

void hdr_getnow(hdr_timespec* t);

double hdr_timespec_as_double(const hdr_timespec* t);

/* Convert value (seconds, with fractional part) into *t, reporting invalid input.
 * Assumes only millisecond accuracy (applies to both variants below).
 * Returns 0 on success, -EINVAL if value is not finite, or -ERANGE if value (or
 * its rounded result) does not fit the destination tv_sec. On failure *t is unchanged. */
int hdr_timespec_from_double_checked(hdr_timespec* t, double value);

/* As hdr_timespec_from_double_checked but with no error return; on failure *t is
 * set to zero. Retained for compatibility. Prefer the checked variant when you need to detect bad input. */
void hdr_timespec_from_double(hdr_timespec* t, double value);

#ifdef __cplusplus
}
#endif

#endif
