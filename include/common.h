#ifndef _COMMON_H_
#define _COMMON_H_

#include <limits.h>

#define perr(x) \
fprintf(stderr, "[ERROR] ***Runtime Error***\nin " __FILE__ ":%d\nError[%d]: %s\n" x, __LINE__, errno, strerror(errno)); \
lprintf("[ERROR] ***Runtime Error***\nin " __FILE__ ":%d\nError[%d]: %s\n" x, __LINE__, errno, strerror(errno));  

#define perrf(x, ...) \
fprintf(stderr, "[ERROR] ***Runtime Error***\nin " __FILE__ ":%d\nError[%d]: %s\n" x, __LINE__, errno, strerror(errno), __VA_ARGS__); \
lprintf("[ERROR] ***Runtime Error***\nin " __FILE__ ":%d\nError[%d]: %s\n" x, __LINE__, errno, strerror(errno), __VA_ARGS__ );  

#define MMAGIC 4272487958703639605
#define YOURNAME_MAX 64

#define NMSZ 64
#define DRSZ 256
#define FILENAMESZ 64
#define CMDSZ 128

#define DISCOVER_PORT 8585
#define LISTEN_QUEUE_LEN 10
#define DATA_BUFF_MAX 864

#define min(a,b) ((a)>(b) ? (b) : (a))
#define max(a,b) ((a)>(b) ? (a) : (b))

#define LIST_CREATE(type,listname) \
	uint32_t listname##_cnt =0; \
	type* listname =NULL; \
	type* listname##_last =NULL

#define LIST_CREATE_C(type,listname) \
	uint32_t listname##_cnt; \
	type* listname; \
	type* listname##_last

#define LIST_APPEND(listname,node) \
	if(listname##_cnt == 0){ \
		listname = node;\
	} else { \
		listname##_last->next = node; \
	} \
	node->prev = listname##_last; \
	listname##_cnt++; \
	listname##_last = node

#define LIST_APPEND_FRONT(listname, node) \
	if(listname##_cnt == 0){ \
		listname = node; \
	} else { \
		listname->prev = node; \
		node->next = listname; \
		listname = node; \
	} \
	listname->prev = NULL; \
	listname##_cnt++

#define LIST_REMOVE(listname,node) \
	if(node->prev != NULL) node->prev->next = node->next; \
	else listname = node->next; \
	if(node->next != NULL) node->next->prev = node->prev; \
	else listname##_last = node->prev; \
	listname##_cnt--

struct maester_conn_data{
	uint64_t magic;
	char addr[16];
	uint16_t port;
	char maestername[YOURNAME_MAX];
};

//msg types

#define MSG_EMPTY 0
#define CLIENT_ADD_JOB 1 
#define MSG_STRING 2
#define CLIENT_JOB_JSON 3
#define CLIENT_JOB_EXE 4
#define MAESTER_CONFIRM 5 
#define MAESTER_OK 6
#define MAESTER_AUTH 7
#define MAESTER_NOPE 8
#define MAESTER_JOB 9
#define MAESTER_UPDATE 10
#define MSG_FILE 11
#define MAESTER_KILL 12
#define WORKER_JOB_DONE 13
#define CLIENT_LIST_JOBS 14
#define MAESTER_NEXT 15
#define CLIENT_LOGS 16
#define CLIENT_TOP 17

//client states
#define CLIENT_STATE_IDLE 0
#define CLIENT_STATE_JSON_FILENAME 2
#define CLIENT_STATE_JSON_FILE 5
#define CLIENT_STATE_JSON_FILE_R 6
#define CLIENT_STATE_EXE_FILENAME_HEADER 1
#define CLIENT_STATE_EXE_FILENAME 7
#define CLIENT_STATE_EXE_FILE 8
#define CLIENT_STATE_EXE_FILE_R 9
#define CLIENT_STATE_DISPATCH 10
#define CLIENT_STATE_DISPATCH_2 11
#define CLIENT_STATE_ADDED 12
#define CLIENT_STATE_ADDED_2 13
#define CLIENT_STATE_DISCARD 14
#define CLIENT_STATE_DISCARD_2 15
#define CLIENT_STATE_STALL 16
#define CLIENT_STATE_EXIT 17
#define CLIENT_STATE_LIST 18
#define CLIENT_STATE_LIST_R 19
#define CLIENT_STATE_LOGS 20
#define CLIENT_STATE_LOGS_R 21
#define CLIENT_STATE_TOP 22

//job status
#define JOB_STATUS_WAITING 0
#define JOB_STATUS_RUNNING 1
#define JOB_STATUS_DISCARDED 2
#define JOB_STATUS_ENDED 3
#define JOB_STATUS_ORPHANED 4

// special flags
#define CONFIRM_JOB_ADDED 12
#define CONFIRM_JOB_DISCARDED 15
#define CONFIRM_JOB_QUEUED 16

// worker_states
#define WORKER_STATE_IDLE 0
#define WORKER_STATE_UPDATE 1
#define WORKER_STATE_JOB_NODE 2
#define WORKER_STATE_EXE_FILE 3
#define WORKER_STATE_EXE_FILE_R 4
#define WORKER_STATE_OFFER 5
#define WORKER_STATE_TERMINATE 6

struct msg_header {
	int64_t content_length;
	uint16_t type;
	int64_t flags;
};

struct conn_info{
	uint8_t type;
	char name[64];
	char cpuname[256];
	char passwd[17];
	char hostname[HOST_NAME_MAX];
	uint16_t cpucnt;
	uint32_t memtotal;
	uint32_t memfree;
	uint32_t max_jobs;
};

struct job_add_confirm{
	uint16_t job_id;
};

struct maester_confirm{
	uint16_t type;
	char msg[256];
	uint64_t worker_id;
	uint64_t job_id;
};

struct jobinfo{
	char cmd[CMDSZ];
	char dir[DRSZ];
	uint16_t limcpucnt;
	uint32_t limmem;
};

struct job_node{
	int64_t id;
	char jsonfilename[FILENAMESZ];
	char exefilename[FILENAMESZ];
	uint32_t exefilesize;
	char tempname[FILENAMESZ];
	char* json_str;
	struct jobinfo ji;
	int16_t status;
	int16_t type;
	int32_t owner;
	struct worker_node* worker;
	int64_t worker_id;
	struct job_node* next;
	struct job_node* prev;
	int tempfd; // network unsafe types
	int fifofd;
	int exit_status;
};

struct job_list_entry{
	int64_t id;
	char workername[64];
	int16_t status;
};

struct worker_top{
	char name[64];
	char hostname[HOST_NAME_MAX];
	char ipstr[16];
	uint16_t cpucnt;
	uint32_t memtotal;
	uint32_t memfree;
	uint32_t max_jobs;
	uint32_t jobs_cnt;
};

int ssend(int, void*, int, int);
int srecv(int, void*, int, int);

#endif
