#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>
#include <aio.h>
#include <arpa/inet.h>
#include <signal.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>

#include "common.h"
#include "logging.h"
#include "maester.h"
#include "cJSON.h"

char* appname;

struct conn_info myinfo;
int comsock;
int detsock;
int max_workers = 4;
int max_jobs = 20;
int max_clients = 5;
int _auth = 1;

int usrs_cnt;
char users[USER_LIM][64];

int _exitsigrecved =0;

uint64_t tempindex =16372;
uint64_t jobids = 13322;
uint64_t workerids = 647;

/////// data structures
pthread_mutex_t worker_lock;
LIST_CREATE(struct worker_node, worker_list);
int worker_maxsockn =0;
pthread_mutex_t client_lock;
LIST_CREATE(struct client_node, client_list);
int client_maxsockn =0;
pthread_mutex_t task_lock;
LIST_CREATE(struct task_node, task_list);
pthread_mutex_t job_lock;
LIST_CREATE(struct job_node, job_list);

pthread_mutex_t offer_lock;
struct job_node* offered_job;
uint16_t should_offer =0;
/*
int worker_list_cnt;
struct worker_node* worker_list;
struct worker_node* worker_list_last;
*/
/////// end

char* getuser(int userno){
	if(_auth){
		return users[userno];
}else return "guest";
}

void print_usage(){
	printf("Usage:\n%s -n name -d base-dir -l log-file -c config-file -p detection-port -s communication-port\n", appname);
	return;
}

static void siginthand(int sig){
	_exitsigrecved = 1;
	close(detsock);
	close(comsock);
	return;
}

int check_auth(char* buff){
	for(int i=0;i<usrs_cnt;i++){
		if(!strcmp(users[i], buff)) return i;
	}
	return -1;
}

int parse_json(struct job_node* jn){
	if(strlen(jn->json_str) == 0){
		perrf("Error parsing json for %lu (too short)\n", jn->id);
		return -1;
	}
	cJSON* data = cJSON_Parse(jn->json_str);
	if(data == NULL){
		perrf("Error json format error for %lu\n", jn->id);
		return -1;
	}
	cJSON* temp;
	jn->ji.dir[0] = '.';
	jn->ji.dir[1] = '/';
	jn->ji.dir[2] = '\0';
	jn->ji.limcpucnt = 0;
	jn->ji.limmem =0;
	temp = cJSON_GetObjectItemCaseSensitive(data, "cmd");
	if(cJSON_IsString(temp) && (temp->valuestring != NULL)){
		strcpy(jn->ji.cmd, temp->valuestring);
	}else{
		perrf("Error parsing json for %lu (cmd)\n", jn->id);
		return -1;
	}
	temp = cJSON_GetObjectItemCaseSensitive(data, "dir");
	if(cJSON_IsString(temp) && (temp->valuestring != NULL)){
		strcpy(jn->ji.dir, temp->valuestring);
	}
	temp = cJSON_GetObjectItemCaseSensitive(data, "cores");
	if(cJSON_IsNumber(temp)){
		jn->ji.limcpucnt = temp->valueint;
	}
	temp = cJSON_GetObjectItemCaseSensitive(data, "memory");
	if(cJSON_IsNumber(temp)){
		jn->ji.limmem = temp->valueint;
	}
}

int offer_job(struct worker_node* wn, struct task_node* tn){
	pthread_mutex_lock(&offer_lock);
	should_offer = 1;
	offered_job = tn->job;
	offered_job->worker = wn;
	pthread_mutex_unlock(&offer_lock);
	return 0;
}

int _update_workers = 0;

void sigalrmhand(int sig){
	_update_workers = 1;
}

void* worker_manager(void* args){
	timer_t timerid;
	struct sigevent timer_sigevent;
	timer_sigevent.sigev_notify = SIGEV_SIGNAL;
	timer_sigevent.sigev_signo = SIGALRM;
	timer_sigevent.sigev_value.sival_ptr = NULL;
	if(timer_create(CLOCK_REALTIME, &timer_sigevent, &timerid) == -1){
		perr("Cant create timer!\n");
		exit(1);
	}
	signal(SIGALRM, sigalrmhand);
	struct itimerspec its;
	its.it_value.tv_sec = 0;
	its.it_value.tv_nsec = 400000;
	its.it_interval.tv_sec = 5;
	its.it_interval.tv_nsec = 0;
	if(timer_settime(timerid, 0, &its, NULL) == -1){
		perr("Cant start timer!\n");
		exit(1);
	}
	struct timeval tv;
	fd_set rdfs;
	fd_set wrfs;
	int ret;
	while(!_exitsigrecved){
		tv.tv_sec = 2;
		tv.tv_usec = 0;
		FD_ZERO(&rdfs);
		FD_ZERO(&wrfs);
		pthread_mutex_lock(&worker_lock);
		struct worker_node* pworker_list = worker_list;
		struct worker_node* pworker_list_last = worker_list_last;
		uint32_t pworker_list_cnt = worker_list_cnt; // use a part of the list(local list) to avoid conflict and create concurrency
		pthread_mutex_unlock(&worker_lock);
		struct worker_node* temp = pworker_list;
		for(int i=0;i<pworker_list_cnt;i++){
			switch(temp->state){
				case WORKER_STATE_IDLE:
					if(should_offer && offered_job->worker == temp){
						temp->state = WORKER_STATE_OFFER;
						FD_SET(temp->socket, &wrfs);
					}else if(_update_workers){
						temp->state = WORKER_STATE_UPDATE;
						FD_SET(temp->socket, &wrfs);
					}else{
						FD_SET(temp->socket, &rdfs);
					}
					worker_maxsockn = (worker_maxsockn > temp->socket ? worker_maxsockn : temp->socket);
				break;
				default:	
					FD_SET(temp->socket, &wrfs);
					worker_maxsockn = (worker_maxsockn > temp->socket ? worker_maxsockn : temp->socket);
				break;
			}
			temp = temp->next;
		}
		_update_workers = 0;
		ret = select(worker_maxsockn+1, &rdfs, &wrfs, NULL, &tv);
		if(ret<0){
			perr("Error at select");
			continue;
		}else if(ret == 0){
			printf("nothing worker\n"); // bruh
			fflush(stdout);
			continue;
		}
		printf("alright worker\n"); // bruh
		fflush(stdout);
		temp = pworker_list;
		int shallproceed = 1;
		for(int i=0;(i<pworker_list_cnt) && shallproceed; i++){
			switch(temp->state){
				case WORKER_STATE_UPDATE:
					temp->state = WORKER_STATE_IDLE;
				break;
				case WORKER_STATE_IDLE:
					if(FD_ISSET(temp->socket, &rdfs)){
						if(srecv(temp->socket, &(temp->mhead), sizeof(temp->mhead), 0)){
							perrf("Error communicating with worker at %s on %s in state IDLE(read)\n", temp->ipstr, temp->hostname);
							temp->state = WORKER_STATE_TERMINATE;
							break;
						}
						switch(temp->mhead.type){
							case WORKER_JOB_DONE:
							;
							struct job_node* jn;
							for(jn=job_list; jn != NULL; jn=jn->next) if(jn->id == temp->mhead.flags) break;
							if(jn == NULL){
									perr("WHAT?! in job done check. ignoring\n");
									break;
							}
							jn->status = JOB_STATUS_ENDED;
						}
					}
				break;
				case WORKER_STATE_OFFER:
					if(FD_ISSET(temp->socket, &wrfs)){
						printf("offering to worker\n"); // bruh
						fflush(stdout);
						struct msg_header mh;
						mh.type = MAESTER_JOB;
						mh.content_length =0;
						if(ssend(temp->socket, &mh, sizeof(mh), 0)){
							perrf("Error communicating with worker %s (id %ld) at %s on %s in state IDLE(dispatch)\n", temp->name, temp->id, temp->ipstr, temp->hostname);
							temp->state = WORKER_STATE_TERMINATE;
							break;
						}
						pthread_mutex_lock(&offer_lock);
						should_offer =0;
						pthread_mutex_unlock(&offer_lock);
						temp->offered_job = offered_job;
						LIST_APPEND(temp->jobs, offered_job);
						temp->state = WORKER_STATE_JOB_NODE;
					}				
				break;
				case WORKER_STATE_JOB_NODE:
					if(FD_ISSET(temp->socket, &wrfs)){
						if(ssend(temp->socket, temp->offered_job, sizeof(struct job_node), 0)){
							perrf("Error communicating with worker at %s on %s in state JOB_NODE\n", temp->ipstr, temp->hostname);
							temp->state = WORKER_STATE_TERMINATE;
							break;
						}
						if(temp->offered_job->type){
							temp->state = WORKER_STATE_EXE_FILE;
						}else{ // we're done
							temp->offered_job->status = JOB_STATUS_RUNNING;
							temp->state = WORKER_STATE_IDLE;
							lprintf("Send job %lu from user %s to worker %lu at %s\n",
							temp->offered_job->id,
							users[temp->offered_job->owner],
							temp->id,
							temp->ipstr);
						}
					}
				break;
				case WORKER_STATE_EXE_FILE:
					if(FD_ISSET(temp->socket, &wrfs)){
						temp->remaining_bytes = temp->offered_job->exefilesize;
						if((temp->offered_job->tempfd = open(temp->offered_job->tempname, O_RDWR)) == -1){
							perrf("Error opening tempfile %s. While communicating with worker at %s.\n", temp->offered_job->tempname, temp->ipstr);
							temp->state = WORKER_STATE_TERMINATE;
							break;
						}
						temp->state = WORKER_STATE_EXE_FILE_R;
					}
				break;
				case WORKER_STATE_EXE_FILE_R:
					if(FD_ISSET(temp->socket, &wrfs)){
						int read_bytes = read(temp->offered_job->tempfd, temp->data_buff, DATA_BUFF_MAX);
						if(read_bytes < 0){
							perrf("Error reading from temp file %s. while communcating with worker at %s\n", temp->offered_job->tempname, temp->ipstr);
							temp->state = WORKER_STATE_TERMINATE;
							break;
						}
						if(ssend(temp->socket, temp->data_buff, read_bytes, 0)){
							perrf("Error communicating with worker at %s on %s in state JOB_NODE\n", temp->ipstr, temp->hostname);
							temp->state = WORKER_STATE_TERMINATE;
							break;
						}
						temp->remaining_bytes -= read_bytes;
						if(temp->remaining_bytes <= 0){
							temp->offered_job->status = JOB_STATUS_RUNNING;
							temp->state = WORKER_STATE_IDLE;
							sclose(temp->offered_job->tempfd);
							lprintf("Send job %lu from user %s to worker %s (id %ld) at %s on %s\n",
								temp->offered_job->id,
								users[temp->offered_job->owner],
								temp->name,
								temp->id,
								temp->ipstr,
								temp->hostname);
						}
					}
				break;
				case WORKER_STATE_TERMINATE:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct msg_header mh;
						mh.type = MAESTER_KILL;
						mh.flags = 1; // indicating error
						mh.content_length =0;
						if(ssend(temp->socket, &mh, sizeof(mh), 0)){
							perrf("Error safely terminating the worker at %s on %s. Erasing worker.\n", temp->ipstr, temp->hostname);
							close(temp->socket);
							pthread_mutex_lock(&worker_lock);
							LIST_REMOVE(worker_list, temp);
							pthread_mutex_unlock(&worker_lock);
							for(struct job_node* jn=temp->jobs; jn!=NULL; jn = jn->next){
								jn->status = JOB_STATUS_ORPHANED;
							}
							free(temp);
							temp = NULL;
							shallproceed = 0;
							break;
						}
					}
				break;
			}
			if(temp) temp = temp->next;
		}
	}
	sleep(0.001);
}

void* task_manager(void* args){
	while(!_exitsigrecved){
		pthread_mutex_lock(&task_lock);
		struct task_node* ptask_list = task_list;
		struct task_node* ptask_list_last = task_list_last;
		uint32_t ptask_list_cnt = task_list_cnt;
		pthread_mutex_unlock(&task_lock);
		struct task_node* tn = ptask_list;
		for(int i=0;i<ptask_list_cnt;i++){
			switch(tn->command){
				case COMMAND_JOB_ADD:
					printf("alright task manager\n"); // bruh
					fflush(stdout);
					parse_json(tn->job);
					pthread_mutex_lock(&worker_lock);
					struct worker_node* temp = worker_list;
					int eligible = 0;
					int delivered = 0;
					while(temp != NULL){
						if(temp->cpucnt > tn->job->ji.limcpucnt &&
							temp->memtotal > tn->job->ji.limmem){
							eligible = 1;
							printf("alright eligible %d\n", should_offer); //bruh
							fflush(stdout);
							if(temp->memfree > tn->job->ji.limmem && temp->jobs_cnt < temp->max_jobs 
								&& !should_offer){
								if(offer_job(temp, tn) == 0){
									printf("alright delivered\n"); //bruh
									fflush(stdout);
									delivered = 1;
									break;
								}
							}
						}
						temp = temp->next;
					}
					if(!eligible){
						tn->job->status = JOB_STATUS_DISCARDED;
						LIST_REMOVE(task_list, tn);
						free(tn);
					}else if(delivered){
						LIST_REMOVE(task_list, tn);
						free(tn);
					}
					pthread_mutex_unlock(&worker_lock);
				break;
			}
			tn = tn->next;
		}
		sleep(0.001);
	}
}

void* client_manager(void* args){
	struct timeval tv;
	fd_set rdfs;
	fd_set wrfs;
	int ret;
	while(!_exitsigrecved){
		tv.tv_sec = 2;
		tv.tv_usec = 0;
		FD_ZERO(&rdfs);
		FD_ZERO(&wrfs);
		pthread_mutex_lock(&client_lock);
		struct client_node* pclient_list = client_list;
		struct client_node* pclient_list_last = client_list_last;
		uint32_t pclient_list_cnt = client_list_cnt; // use a part of the list(local list) to avoid conflict and create concurrency
		pthread_mutex_unlock(&client_lock);
		struct client_node* temp = pclient_list;
		int shallproceed = 1;
		for(int i=0;i<pclient_list_cnt;i++){
			if(temp->active)
			switch(temp->state){
				case CLIENT_STATE_DISPATCH_2:
				case CLIENT_STATE_DISPATCH:
				case CLIENT_STATE_DISCARD:
				case CLIENT_STATE_DISCARD_2:
				case CLIENT_STATE_ADDED:
				case CLIENT_STATE_ADDED_2:
				case CLIENT_STATE_LIST:
				case CLIENT_STATE_LIST_R:
				case CLIENT_STATE_LOGS:
				case CLIENT_STATE_LOGS_R:
				case CLIENT_STATE_TOP:
					FD_SET(temp->socket, &wrfs);
					client_maxsockn = (client_maxsockn > temp->socket ? client_maxsockn : temp->socket);
					temp->isinset = 1;
				break;
				case CLIENT_STATE_STALL:
					if(temp->iji->status == JOB_STATUS_RUNNING || temp->iji->status == JOB_STATUS_ENDED){
						temp->state = CLIENT_STATE_ADDED;
						printf("alright stall -> added\n"); // bruh
						fflush(stdout);
					}else if(temp->iji->status == JOB_STATUS_DISCARDED){
						temp->state = CLIENT_STATE_DISCARD;
						printf("alright stall -> discarded\n"); // bruh
						fflush(stdout);
					}
				break;
				case CLIENT_STATE_EXIT:
					printf("alright exit\n"); // bruh
					fflush(stdout);
					temp->active=0;
					if(temp->action == CLIENT_ADD_JOB) free(temp->iji->json_str);
					sclose(temp->socket);
					pthread_mutex_lock(&client_lock);
					LIST_REMOVE(client_list, temp); 
					pthread_mutex_unlock(&client_lock);
					shallproceed = 0; // local list is invalid, start over!
				break;
				default:	
					FD_SET(temp->socket, &rdfs);
					client_maxsockn = (client_maxsockn > temp->socket ? client_maxsockn : temp->socket);
					temp->isinset = 1;
				break;
			}
			temp = temp->next;
		}
		if(!shallproceed) continue;
		ret = select(client_maxsockn+1, &rdfs, &wrfs, NULL, &tv);
		if(ret<0){
			perr("Error at select");
			continue;
		}else if(ret == 0){
			printf("nothing...\n"); // bruh
			fflush(stdout);
			continue;
		}
		printf("got it!\n"); // bruh
		fflush(stdout);
		temp = pclient_list;
		for(int i=0;(i<pclient_list_cnt) && shallproceed;i++){
			switch(temp->state){
				case CLIENT_STATE_IDLE:
					if(FD_ISSET(temp->socket, &rdfs)){
						if(srecv(temp->socket, &(temp->rdata_mh), sizeof(struct msg_header), 0)){
							perrf("Error communicating with client at %s on %s at state IDLE for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						temp->action = temp->rdata_mh.type;
						switch(temp->rdata_mh.type){
							case CLIENT_ADD_JOB:
								printf("Alright 1\n");
								fflush(stdout);
								temp->state = CLIENT_STATE_JSON_FILENAME;
								temp->iji = (struct job_node*)malloc(sizeof(struct job_node));
								pthread_mutex_lock(&job_lock);
								LIST_APPEND(job_list, temp->iji);
								pthread_mutex_unlock(&job_lock);
								temp->iji->type = temp->rdata_mh.flags;
								lprintf("Client at %s on %s is attempting to add a job\n", temp->ipstr, temp->hostname);
							break;
							case CLIENT_LIST_JOBS:
								temp->state = CLIENT_STATE_LIST;
							break;
							case CLIENT_LOGS:
								temp->state = CLIENT_STATE_LOGS;
							break;
							case CLIENT_TOP:
								temp->toplist = worker_list;
								temp->state = CLIENT_STATE_TOP;
							break;
						}
					}
				break;
				case CLIENT_STATE_TOP:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct msg_header mh;
						if(temp->toplist == NULL){
							mh.type=MSG_EMPTY;
							ssend(temp->socket, &mh, sizeof(mh),0);
							temp->state = CLIENT_STATE_EXIT;
						}else{
							mh.type=MAESTER_OK;
							ssend(temp->socket, &mh, sizeof(mh),0);
							struct worker_top wt;
							strcpy(wt.name, temp->toplist->name);
							strcpy(wt.hostname, temp->toplist->hostname);
							strcpy(wt.ipstr, temp->toplist->ipstr);
							wt.cpucnt = temp->toplist->cpucnt;
							wt.memtotal = temp->toplist->memtotal;
							wt.memfree = temp->toplist->memfree;
							wt.max_jobs = temp->toplist->max_jobs;
							wt.jobs_cnt = temp->toplist->jobs_cnt;
							ssend(temp->socket, &wt, sizeof(wt),0);
							temp->toplist = temp->toplist->next;
						}
					}
				break;
				case CLIENT_STATE_LOGS:
					if(FD_ISSET(temp->socket, &wrfs)){
						
					}
				break;
				case CLIENT_STATE_LOGS_R:
					if(FD_ISSET(temp->socket, &wrfs)){

					}
				break;
				case CLIENT_STATE_LIST:
					if(FD_ISSET(temp->socket, &wrfs)){
						temp->listptr = job_list;
						struct msg_header mh;
						if(job_list == NULL){
							mh.type = MSG_EMPTY;
							if(ssend(temp->socket, &mh, sizeof(mh),0)){
								perrf("Error communicating with client at %s on %s at state LIST for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
								temp->state = CLIENT_STATE_EXIT;
								continue;
							}
						}else{
							mh.type = MAESTER_NEXT;
							if(ssend(temp->socket, &mh, sizeof(mh),0)){
								perrf("Error communicating with client at %s on %s at state LIST for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
								temp->state = CLIENT_STATE_EXIT;
								continue;
							}
							temp->state = CLIENT_STATE_LIST_R;
						}
							
					}
				break;
				case CLIENT_STATE_LIST_R:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct job_list_entry jle;
						jle.id = temp->listptr->id;
						strcpy(jle.workername, temp->listptr->worker->name);
						jle.status = temp->listptr->status;
						if(ssend(temp->socket, &jle, sizeof(jle), 0)){
							perrf("Error communicating with client at %s on %s at state LIST_R for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							temp->state = CLIENT_STATE_EXIT;
							continue;
						}
						temp->listptr = temp->listptr->next;
					}
					struct msg_header mh;
					if(temp->listptr == NULL){
						mh.type = MSG_EMPTY;
						if(ssend(temp->socket, &mh, sizeof(mh),0)){
							perrf("Error communicating with client at %s on %s at state LIST_R for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							temp->state = CLIENT_STATE_EXIT;
							continue;
						}
						temp->state = CLIENT_STATE_IDLE;
					}else{
						mh.type = MAESTER_NEXT;
						if(ssend(temp->socket, &mh, sizeof(mh),0)){
							perrf("Error communicating with client at %s on %s at state LIST_R for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							temp->state = CLIENT_STATE_EXIT;
							continue;
						}
						temp->state = CLIENT_STATE_LIST_R;
					}
				break;
				case CLIENT_STATE_JSON_FILENAME:
					if(FD_ISSET(temp->socket, &rdfs)){
						temp->remaining_bytes = temp->rdata_mh.content_length;
						if(srecv(temp->socket, temp->iji->jsonfilename, temp->remaining_bytes, 0)){
							perrf("Error communicating with client at %s on %s at state JSON_FILENAME for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						printf("alright json file name\n"); // bruh
						fflush(stdout);
						temp->state = CLIENT_STATE_JSON_FILE;
					}
				break;
				case CLIENT_STATE_JSON_FILE:
					if(FD_ISSET(temp->socket, &rdfs)){
						if(srecv(temp->socket, &(temp->rdata_mh), sizeof(struct msg_header), 0)){
							perrf("Error communicating with client at %s on %s at state JSON_FILE for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							continue;
						}
						printf("alright json file\n"); // bruh
						fflush(stdout);
						temp->state = CLIENT_STATE_JSON_FILE_R;
						temp->remaining_bytes = temp->rdata_mh.content_length;
						temp->iji->json_str = (char*)malloc(temp->remaining_bytes);
						if(temp->iji->json_str == NULL){
							perrf("MALLOC ERROR! at %s on %s at state JSON_FILE for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						temp->rbuffptr = temp->iji->json_str;
					}
				break;
				case CLIENT_STATE_JSON_FILE_R:
					if(FD_ISSET(temp->socket, &rdfs)){
						//temp->rbuffptr = temp->iji->json_str;
						printf("file r\n"); // bruh
						fflush(stdout);
						int read_bytes = recv(temp->socket, temp->rbuffptr, temp->remaining_bytes, 0);
						printf("file r: %d : %s\n", read_bytes, temp->rbuffptr); // bruh
						fflush(stdout);
						if(temp->remaining_bytes < 0){
							perrf("Error communicating with client at %s on %s at state JSON_FILE_R for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						temp->remaining_bytes -= read_bytes;
						temp->rbuffptr += read_bytes;
						if(temp->remaining_bytes <= 0){
							if(temp->iji->type){ // is exe
								printf("alright json file(exe)\n"); // bruh
								fflush(stdout);
								temp->state = CLIENT_STATE_EXE_FILENAME_HEADER;
							}else{
								printf("alright json file r\n"); // bruh
								fflush(stdout);
								temp->iji->status = JOB_STATUS_WAITING;
								temp->iji->owner = temp->usr;
								temp->iji->id = jobids++;
								struct task_node* tn = (struct task_node*)malloc(sizeof(struct task_node));
								tn->command = COMMAND_JOB_ADD;
								tn->job = temp->iji;
								pthread_mutex_lock(&task_lock);
								LIST_APPEND_FRONT(task_list, tn);
								pthread_mutex_unlock(&task_lock);
								temp->state = CLIENT_STATE_DISPATCH;
							}
						}
					}
				break;
				case CLIENT_STATE_EXE_FILENAME_HEADER:
					if(FD_ISSET(temp->socket, &rdfs)){
						if(srecv(temp->socket, &(temp->rdata_mh), sizeof(struct msg_header), 0)){
							perrf("Error communicating with client at %s on %s at state EXE_FILENAME(header) for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						printf("alright filename header %d\n", temp->rdata_mh.content_length); // bruh
						fflush(stdout);
						temp->remaining_bytes = temp->rdata_mh.content_length;
						temp->state = CLIENT_STATE_EXE_FILENAME;
					}
				break;
				case CLIENT_STATE_EXE_FILENAME:
					if(FD_ISSET(temp->socket, &rdfs)){
						if(srecv(temp->socket, temp->iji->exefilename, temp->remaining_bytes, 0)){
							perrf("Error communicating with client at %s on %s at state EXE_FILENAME(string) for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						sprintf(temp->iji->tempname, "/tmp/tempfile%lu", tempindex++);
						if((temp->iji->tempfd = open(temp->iji->tempname, O_RDWR | O_CREAT | O_TRUNC, 0664)) == -1){
							perrf("Error creating tempfile for client at %s on %s at state EXE_FILENAME(string) for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						printf("alright exe filename\n"); // bruh
						fflush(stdout);
						temp->state = CLIENT_STATE_EXE_FILE;
					}
				break;
				case CLIENT_STATE_EXE_FILE:
					if(FD_ISSET(temp->socket, &rdfs)){
						if(srecv(temp->socket, &(temp->rdata_mh), sizeof(struct msg_header), 0)){
							perrf("Error communicating with client at %s on %s at state EXE_FILE(header) for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						temp->remaining_bytes = temp->rdata_mh.content_length;
						printf("alright exe file %d \n", temp->remaining_bytes);
						temp->iji->exefilesize = temp->rdata_mh.content_length;
						temp->state = CLIENT_STATE_EXE_FILE_R;
					}
				break;
				case CLIENT_STATE_EXE_FILE_R:
					if(FD_ISSET(temp->socket, &rdfs)){
						int read_bytes = recv(temp->socket, temp->rdata_buff, DATA_BUFF_MAX, 0);
						if(read_bytes < 0){
							perrf("Error communicating with client at %s on %s at state EXE_FILE(file) for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						temp->remaining_bytes -= read_bytes;
						if(write(temp->iji->tempfd, temp->rdata_buff, read_bytes) == -1){
							perrf("Error writing to temp file for client at %s on %s at state EXE_FILE(file) for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
							// TODO: send error message
							temp->state = CLIENT_STATE_DISCARD;
							continue;
						}
						if(temp->remaining_bytes <= 0){
							temp->iji->status = JOB_STATUS_WAITING;
							temp->iji->owner = temp->usr;
							temp->iji->id = jobids++;
							sclose(temp->iji->tempfd);
							struct task_node* tn = (struct task_node*)malloc(sizeof(struct task_node));
							tn->command = COMMAND_JOB_ADD;
							tn->job = temp->iji;
							pthread_mutex_lock(&task_lock);
							LIST_APPEND_FRONT(task_list, tn);
							pthread_mutex_unlock(&task_lock);
							printf("alright exe file r\n"); // bruh
							fflush(stdout);
							temp->state = CLIENT_STATE_DISPATCH;
						}
					}
				break;
				case CLIENT_STATE_DISPATCH:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct msg_header mh;
						mh.type = MAESTER_CONFIRM;
						mh.flags = CONFIRM_JOB_ADDED;
						mh.content_length =0;
						if(ssend(temp->socket, &mh, sizeof(mh), 0) == -1){
							perrf("Error communicating with client at %s on %s at state DISPATCH for user %s(inast ke bade!)\n", temp->ipstr, temp->hostname, getuser(temp->usr));
						}
						printf("alright dispatch\n"); // bruh
						fflush(stdout);
						temp->state = CLIENT_STATE_DISPATCH_2;
					}
				break;
				case CLIENT_STATE_DISPATCH_2:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct maester_confirm mc;
						mc.type = CONFIRM_JOB_QUEUED;
						if(ssend(temp->socket, &mc, sizeof(mc), 0) == -1){
							perrf("Error communicating with client at %s on %s at state DISPATCH2 for user %s(inast ke bade!)\n", temp->ipstr, temp->hostname, getuser(temp->usr));
						}
						printf("alright dispatch 2\n"); // bruh
						temp->state = CLIENT_STATE_STALL;
					}
				break;
				case CLIENT_STATE_ADDED:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct msg_header mh;
						mh.type = MAESTER_CONFIRM;
						mh.flags = CONFIRM_JOB_ADDED;
						mh.content_length =0;
						if(ssend(temp->socket, &mh, sizeof(mh), 0) == -1){
							perrf("Error communicating with client at %s on %s at state DISPATCH for user %s(inast ke bade!)\n", temp->ipstr, temp->hostname, getuser(temp->usr));
						}
						printf("alright added\n"); // bruh
						fflush(stdout);
						temp->state = CLIENT_STATE_ADDED_2;
					}
				break;
				case CLIENT_STATE_ADDED_2:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct maester_confirm mc;
						mc.type = CONFIRM_JOB_ADDED;
						mc.worker_id = temp->iji->worker->id;
						mc.job_id = temp->iji->id;
						if(ssend(temp->socket, &mc, sizeof(mc), 0) == -1){
							perrf("Error communicating with client at %s on %s at state DISPATCH2 for user %s(inast ke bade!)\n", temp->ipstr, temp->hostname, getuser(temp->usr));
						}
						printf("alright added 2\n"); // bruh
						fflush(stdout);
						temp->state = CLIENT_STATE_EXIT;
					}
				break;
				case CLIENT_STATE_DISCARD:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct msg_header mh;
						mh.type = MAESTER_CONFIRM;
						mh.flags = CONFIRM_JOB_ADDED;
						mh.content_length =0;
						if(ssend(temp->socket, &mh, sizeof(mh), 0) == -1){
							perrf("Error communicating with client at %s on %s at state DISPATCH for user %s\n", temp->ipstr, temp->hostname, getuser(temp->usr));
						}
						temp->state = CLIENT_STATE_DISCARD_2;
					}
				break;
				case CLIENT_STATE_DISCARD_2:
					if(FD_ISSET(temp->socket, &wrfs)){
						struct maester_confirm mc;
						mc.type = CONFIRM_JOB_DISCARDED;
						if(ssend(temp->socket, &mc, sizeof(mc), 0) == -1){
							perrf("Error communicating with client at %s on %s at state DISPATCH2 for user %s(inast ke bade!)\n", temp->ipstr, temp->hostname, getuser(temp->usr));
						}
						temp->state = CLIENT_STATE_EXIT;
					}
				break;
				case CLIENT_STATE_STALL:
				break;
				case CLIENT_STATE_EXIT:
				break;
			}
			temp = temp->next;
		}
		if(shallproceed) sleep(0.01);
	}
}

void* accept_manager(void* args){
	struct sockaddr_in newguy;
	int ngl = sizeof(newguy);
	struct conn_info newinfo;
	int newsock;
	while(! _exitsigrecved){
		if((newsock = accept(comsock, (struct sockaddr*) &newguy, &ngl)) == -1){
			perr("Couldnt accept tcp request.\n");
			continue;
		}
		printf("accepted\n"); // bruh
		if(srecv(newsock, &newinfo, sizeof(newinfo), 0) == -1){
			perr("Couldnt get connection info\n");
			close(newsock);
			continue;
		}
		if(newinfo.type == 0){ // worker
			printf("accpting worker\n"); // bruh
			fflush(stdout);
			if(worker_list_cnt >= max_workers){ // reject worker
				struct msg_header mh;
				mh.type = MAESTER_NOPE;
				if(ssend(newsock, &mh, sizeof(mh), 0) == -1){
					perr("Cant communicate with worker(rejecting)\n");
					continue;
				}
				close(newsock);
				continue;
			}
			printf("adding worker\n"); // bruh
			fflush(stdout);
			struct worker_node* wn = (struct worker_node*)malloc(sizeof(struct worker_node));
			strcpy(wn->name, newinfo.name);
			strcpy(wn->cpuname, newinfo.cpuname);
			strcpy(wn->hostname, newinfo.hostname);
			wn->cpucnt = newinfo.cpucnt;
			wn->memtotal = newinfo.memtotal;
			wn->memfree = newinfo.memfree;
			wn->socket = newsock;
			wn->max_jobs = newinfo.max_jobs;
			wn->state = WORKER_STATE_IDLE;
			wn->slay_count = 0;
			wn->id = workerids++;
			wn->jobs = NULL;
			wn->jobs_last = NULL;
			wn->jobs_cnt = 0;
			memcpy(&(wn->address), &newguy, sizeof(newguy));
			wn->addrlen = sizeof(newguy);
			inet_ntop(AF_INET, (struct sockaddr*) &newguy, wn->ipstr, sizeof(newguy));
			wn->next = NULL;
			pthread_mutex_lock(&worker_lock);
			LIST_APPEND(worker_list, wn);
			lprintf("Accepted a worker at %s on %s\n", wn->ipstr, wn->hostname);
			pthread_mutex_unlock(&worker_lock);
			struct msg_header mh;
			mh.type = MAESTER_OK;
			if(ssend(newsock, &mh, sizeof(mh), 0) == -1){
				perr("Cant communicate with worker(accepting)\n");
				continue;
			}
		}else if(newinfo.type == 1){ // client
			struct msg_header mh;
			int userno = -1; // guest
			if(client_list_cnt >= max_clients){
				mh.type = MAESTER_NOPE;
				if(ssend(newsock, &mh, sizeof(mh), 0) == -1){
					perr("Cant communicate with client(rejecting)\n");
					continue;
				}
				close(newsock);
				continue;
			}
			if(_auth){
				mh.type = MAESTER_AUTH;
				if(ssend(newsock, &mh, sizeof(mh), 0) == -1){
					perr("Cant communicate with client(authing)\n");
					close(newsock);
					continue;
				}
				if(srecv(newsock, &mh, sizeof(mh), 0) == -1){
					perr("Cant communicate with client(authing)\n");
					close(newsock);
					continue;
				}
				if(mh.type != MSG_STRING){
				perr("what?!\n");
					close(newsock);
					continue;
				}
				char buff[64];	
				if(srecv(newsock, buff, mh.content_length, 0) == -1){
					perr("Cant communicate with client(authing)\n");
					close(newsock);
					continue;
				}
				if((userno = check_auth(buff)) == -1){
					mh.type=MAESTER_NOPE;
					mh.flags = 0;
					mh.content_length =0;
					if(ssend(newsock, &mh, sizeof(mh), 0) == -1){
						perr("Cant communicate with client(authing:rejecting)\n");
						close(newsock);
						continue;
					}
					close(newsock);
					continue;
				}
			}
			struct client_node* cn = (struct client_node*) malloc(sizeof(struct client_node));
			cn->socket = newsock;
			cn->active = 1;
			cn->usr = userno;
			cn->state = CLIENT_STATE_IDLE;
			cn->isinset = 0;
			strcpy(cn->hostname, newinfo.hostname);
			memcpy(&(cn->address), &newguy, sizeof(newguy));
			inet_ntop(AF_INET, (struct sockaddr*) &newguy, cn->ipstr, sizeof(newguy));
			pthread_mutex_lock(&client_lock);
			LIST_APPEND(client_list, cn);
			lprintf("Accepted a client at %s on %s for user %s\n", cn->ipstr, cn->hostname, getuser(userno));
			pthread_mutex_unlock(&client_lock);
			mh.type = MAESTER_OK;
			mh.flags = 0;
			mh.content_length = 0;
			if(ssend(newsock, &mh, sizeof(mh), 0) == -1){
				perr("Cant communicate with client(accepting)\n");
				close(newsock);
				continue;
			}
		}else{
			perr("what?\n");
		}
		sleep(0.001);
	}
}

int getmyip(char* buff){
	const char* addr = "8.8.8.8";
	const int port = 53;
	int sockfd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(sockfd < 0) return -1;

	struct sockaddr_in sout;
	sout.sin_family = AF_INET;
	inet_pton(AF_INET, addr, &(sout.sin_addr));
	sout.sin_port = port;

	if(connect(sockfd, (struct sockaddr*) &sout, sizeof(sout)) == -1) return -1;

	struct sockaddr_in name;
	int nlen = sizeof(name);

	if(getsockname(sockfd, (struct sockaddr*) &name, &nlen) == -1) return -1;

	inet_ntop(AF_INET, &(name.sin_addr), buff, sizeof(name));
	close(sockfd);
	return 0;
}

int initialize_maester(){
	if(pthread_mutex_init(&client_lock, NULL) != 0 ){
		perr("Error creating client mutex(pthread)\n");
		return -1;
	}

	if(pthread_mutex_init(&worker_lock, NULL) != 0 ){
		perr("Error creating worker mutex(pthread)\n");
		return -1;
	}
	// initialize data structures
}

int configure_maester(char* homedir, char* configfilename){
	char tmp[256];
	sprintf(tmp, "./getinfo.sh %s", homedir);
	int stat = system(tmp);
	if(! (WIFEXITED(stat) && WEXITSTATUS(stat) == 0)) return -1;
	sprintf(tmp, "%s%s", homedir, "deviceinfo.inf");
	FILE* file = fopen(tmp, "r");
	if(file == NULL) return -1;		
	fgets(tmp, 256, file);
	strcpy(myinfo.cpuname, tmp);
	fscanf(file, "%hd", &(myinfo.cpucnt));
	fscanf(file, "%d", &(myinfo.memtotal));
	fscanf(file, "%d", &(myinfo.memfree));
	if(_auth){
		sprintf(tmp, "%s%s", homedir, "shadow");
		int fd = open(tmp, O_CREAT | O_RDWR, 0600);
		if(fd == -1){
			perr("[warning] Authorization enabled in config but shadow file not found!\nno clients can connect so this is basically useless\n");
			return 1;
		}
		struct stat st;
		fstat(fd, &st);
		if(st.st_size == 0){ // if empty
			write(fd, "admin\n", 6);
		}
		FILE* shadow = fdopen(fd, "r");
		usrs_cnt =0;
		while(usrs_cnt < USER_LIM){
			if(fscanf(shadow, "%s", users[usrs_cnt++]) == EOF) break;
		}
		usrs_cnt --;
		fclose(shadow);
	}
	sprintf(tmp, "%s%s", homedir, configfilename);
	fclose(file);
	file = fopen(tmp, "r");
	if(file == NULL){
		perrf("Warning, could not find config file at %s. Proceeding with default values\n", tmp);
		return 0;
	}
	while(fgets(tmp, 256, file) != NULL){
		if(!strncmp("max_jobs:", tmp, 9)){
			sscanf(tmp, "max_jobs: %d", &max_jobs);
		}else if(!strncmp("max_workers:", tmp, 11))
			sscanf(tmp, "max_workers: %d", &max_workers);
			else if(!strncmp("max_clients:", tmp, 12))
				sscanf(tmp, "max_clients: %d", &max_clients);
				else if(!strncmp("auth:", tmp, 5))
					sscanf(tmp, "auth: %d", &_auth);
					else{
						perrf("Error cannot parse config file. Can't understand \'%s\'", tmp);
						fclose(file);
						return -1;
		}
	}
	fclose(file);
	return 0;
}

int main(int argc, char* argv[]){
	appname = argv[0]; 
	char logfilename[NMSZ] = "maester.log";
	char configfilename[NMSZ] = "maester.conf";
	char myname[NMSZ] = "new_maester";
	strcpy(myinfo.name, myname);
	char homedir[DRSZ] = "./";
	char hostname[HOST_NAME_MAX];
	gethostname(hostname, HOST_NAME_MAX);
	int detport = 8585;
	int comport = 8569;
	pthread_t client_thread, worker_thread, accept_thread, task_thread;
	signal(SIGINT, siginthand);
	signal(SIGPIPE, SIG_IGN); // ignore the sigpipe so the program wont crash. 
							  // write errors are handled properly.
	int opt;
	myinfo.type=0;
	while((opt = getopt(argc, argv, "hc:d:l:n:p:s:"))!= -1){
		switch(opt){
			case 'h':
			print_usage();
				exit(0);
			break;
			case 'n':
				strcpy(myinfo.name, optarg);
			break;
			case 'l':
				strcpy(homedir, optarg);
			break;
			case 'p':
				detport = atoi(optarg);
			break;
			case 's':
				comport = atoi(optarg);
			break;
			case 'd':
				strcpy(logfilename, optarg);
			break;
			case 'c':
				strcpy(configfilename, optarg);
			break;
			case '?':
			print_usage();
				exit(1);
			break;
		}
	}	
	if(initiate_log(homedir, logfilename) == -1){
		perr("Error initiating log\n");
	exit(1);
	}
	lprintf("\nstarting maester %s on %s\ncommunication port: %d - discover port: %d\n", myinfo.name, hostname, comport, detport);
	printf("\nstarting maester %s on %s\ncommunication port: %d - discover port: %d\n", myinfo.name, hostname, comport, detport);
	lprintf("initiated logfile at %s%s\n", homedir, logfilename);
	if(configure_maester(homedir, configfilename) == -1){
		perr("Error configuring maester\n");
	exit(1);
	}
	lprintf("sucessfully configured maester\n");
	struct maester_conn_data mcd;
	mcd.magic = MMAGIC;
	mcd.port = comport;
	strcpy(mcd.maestername, myname);
	if(getmyip(mcd.addr) == -1){
		perr("Coulent resolv local ip\n");
		return -1;
	}
	printf("\t%s\n", mcd.addr);
	if(initialize_maester() == -1){
		perr("Error initalizing maester\n");
	exit(1);
	}
	lprintf("initialization of maester done\n");
	//////// ping socket (discover)
	if((detsock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1){
		perr("Error cant create UDP ping socket\n");
	exit(1);
	}
	struct sockaddr_in s_self;
	s_self.sin_addr.s_addr = INADDR_ANY;
	s_self.sin_family=AF_INET;
	s_self.sin_port = htons(detport);
	if(bind(detsock, (struct sockaddr*)&s_self, sizeof(s_self)) == -1){
		perr("Error binding UDP ping socket\n");
	exit(1);
	}
	struct sockaddr_in s_other;
	int slen = sizeof(s_other);
	uint64_t inputbuff;
	/////// accept socket (communication)
	comsock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(comsock == -1){
		perr("Couldnt create communication socket\n");
	exit(1);
	}
	int reuseenb=1;
	setsockopt(comsock, SOL_SOCKET, SO_REUSEADDR, &reuseenb, sizeof(reuseenb));
	struct sockaddr_in sc_self;
	sc_self.sin_family = AF_INET;
	sc_self.sin_addr.s_addr = INADDR_ANY;
	sc_self.sin_port = htons(comport);
	if(bind(comsock, (struct sockaddr*)&sc_self, sizeof(sc_self)) == -1){
		perr("Couldnt bind communication socket\n");
	exit(1);
	}
	if(listen(comsock, LISTEN_QUEUE_LEN) == -1){
		perr("Listening on communiacation port failed\n");
	exit(1);
	}
	lprintf("maester starting...\n");
	pthread_create(&accept_thread, NULL, accept_manager, NULL);
	pthread_create(&client_thread, NULL, client_manager, NULL);
	pthread_create(&worker_thread, NULL, worker_manager, NULL);
	pthread_create(&task_thread, NULL, task_manager, NULL);
	while(! _exitsigrecved){
		// wait for new worker pings
		if(recvfrom(detsock,  &inputbuff, sizeof(uint64_t), 0, (struct sockaddr*)&s_other, &slen) == -1){
			perr("Error on reading from UDP ping socket\n");
			pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
			pthread_cancel(accept_thread);
			pthread_cancel(client_thread);
			pthread_cancel(worker_thread);
			pthread_cancel(task_thread);
		}
		if(inputbuff == MMAGIC){ // worker found
			if(sendto(detsock, &mcd, sizeof(mcd), 0,(struct sockaddr*) &s_other, slen) == -1){
				perr("Couldnt pong worker\n");
			}
		} // else ignore
	}

	pthread_join(accept_thread, NULL);
	pthread_join(client_thread, NULL);
	pthread_join(worker_thread, NULL);
	pthread_join(task_thread, NULL);
	return 0;
}
