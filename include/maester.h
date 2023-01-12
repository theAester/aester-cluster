#ifndef _MAESTER_H_
#define _MAESTER_H_

#define USER_LIM 10

#define COMMAND_JOB_ADD 1
#define SLAY_THRESHOLD 10

#define sclose(x) printf("%d closing %d\n", __LINE__, x); close(x);

struct task_node{
	uint16_t command;
	struct job_node* job;
	struct task_node* prev;
	struct task_node* next;
};

struct worker_node{
	int64_t id;
	char name[64];
	char cpuname[256];
	char hostname[HOST_NAME_MAX];
	char ipstr[16];
	char data_buff[DATA_BUFF_MAX];
	struct msg_header mhead;
	uint16_t cpucnt;
	uint32_t memtotal;
	uint32_t memfree;
	uint32_t max_jobs;
	uint16_t state;
	uint32_t remaining_bytes;
	uint32_t slay_count;
	struct job_node* offered_job;
	struct sockaddr_in address;
	socklen_t addrlen;
	LIST_CREATE_C(struct job_node, jobs);
	struct worker_node* next;
	struct worker_node* prev;
	int socket;
};

struct client_node{
	uint16_t action;
	char hostname[HOST_NAME_MAX]; // device name
	uint8_t isinset;
	char rdata_buff[DATA_BUFF_MAX]; // read data char type
	char* rbuffptr;
	struct msg_header rdata_mh;  // read data header type
	uint32_t remaining_bytes;
	int usr; // user index
	uint16_t state; // state
	uint8_t active; // is this user client curently active? 
	struct worker_node* toplist;
	struct job_node* iji; // incoming job info
	struct job_node* listptr;
	struct sockaddr_in address;
	char ipstr[16];
	pthread_mutex_t job_lock;
	LIST_CREATE_C(struct job_node, jobs);
	struct client_node* next;
	struct client_node* prev;
	int socket; // socket
};

#endif
