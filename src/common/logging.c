#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <pthread.h>

#include "logging.h"

FILE* _logfile = NULL;
pthread_mutex_t lock;

int initiate_log(char* homedir, char* logfilename){
	if(_logfile) return -1;
	char namebuff[256];
	sprintf(namebuff, "%s%s", homedir, logfilename);
	_logfile = fopen(namebuff, "a");
	if(_logfile == NULL) return -1;
	pthread_mutex_init(&lock, NULL);
	return 0;
} 

void lprintf(const char* format, ...){
	if(_logfile == NULL) return;
	va_list vl;
	va_start(vl, format);
	time_t tm;
	time(&tm);
	#ifdef _LOGGING_TEE_
	vprintf(format, vl);
	va_end(vl);
	va_start(vl, format);
	#endif
	pthread_mutex_lock(&lock);
	fprintf(_logfile, ">>>%s", ctime(&tm));
	vfprintf(_logfile, format, vl);
	fflush(_logfile);
	pthread_mutex_unlock(&lock);
	va_end(vl);
	return;
}
