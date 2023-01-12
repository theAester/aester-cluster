#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <string.h>
#include <aio.h>
#include <arpa/inet.h>
#include <signal.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <sys/stat.h>

#include "common.h"
#include "logging.h"

#define _M_DEBUG_

#define cerr(x) fprintf(stderr, "Error[%d]: %s\nIn " __FILE__ ":%d\n" x, errno, strerror(errno), __LINE__); fflush(stderr)
#define cerrf(x, ...) fprintf(stderr, "Error[%d]: %s\nIn " __FILE__ ":%d\n" x, errno, strerror(errno), __LINE__, __VA_ARGS__); fflush(stderr)
#define CMDBUFLEN 1024
#define CLIENT_PORT 22334
#define LAST_ERR_SZ 256

int maester_sock = -1;

char* json_file_name;
char* executable_name;

int _pongrcved =0;

char lasterr[LAST_ERR_SZ];

void print_usage(char* str){
	printf("Usage:\n%s [command]\n", str);
}

void print_job_help(char* str){
	printf("Usage:\n%s job {add json_file -simple|-execute exefile} | {list} | {logs job_id [-tail|-head=-tail] [-count #lines=5]}\n", str);
}

char *strmbtok ( char *input, char *delimit, char *openblock, char *closeblock) {
    static char *token = NULL;
    char *lead = NULL;
    char *block = NULL;
    int iBlock = 0;
    int iBlockIndex = 0;

    if ( input != NULL) {
        token = input;
        lead = input;
    }
    else {
        lead = token;
        if ( *token == '\0') {
            lead = NULL;
        }
    }

    while ( *token != '\0') {
        if ( iBlock) {
            if ( closeblock[iBlockIndex] == *token) {
                iBlock = 0;
            }
            token++;
            continue;
        }
        if ( ( block = strchr ( openblock, *token)) != NULL) {
            iBlock = 1;
            iBlockIndex = block - openblock;
            token++;
            continue;
        }
        if ( strchr ( delimit, *token) != NULL) {
            *token = '\0';
            token++;
            break;
        }
        token++;
    }
    return lead;
}


static void aioreadhand(int sig, siginfo_t* si, void* ctx){
	if(si->si_code == SI_ASYNCIO){
		printf("read!\n");
		_pongrcved = 1;
	}
}


void await_connection(int onport, struct maester_conn_data *mcd){
	int udpsock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(udpsock == -1){
		cerr("Error creating socket\n");
		exit(1);
	}
	struct sockaddr_in sa_self;
	sa_self.sin_addr.s_addr = INADDR_ANY;
	sa_self.sin_port = htons(onport);
	sa_self.sin_family = AF_INET;
	if(bind(udpsock, (struct sockaddr*)&sa_self, sizeof(sa_self)) == -1){
		cerr("Error binding socket\n");
		exit(1);
	}
	char pingbuff[64];
	//struct worker_handshake pmesg;
	uint64_t pmesg = MMAGIC;
	int broadenb = 1;
	struct sockaddr_in sa;
	sa.sin_family = AF_INET;
	sa.sin_port = htons(DISCOVER_PORT);
	sa.sin_addr.s_addr = INADDR_BROADCAST;
	setsockopt(udpsock,	SOL_SOCKET, SO_BROADCAST, &broadenb, sizeof(broadenb));

	struct sigaction ss;
	ss.sa_flags = SA_RESTART | SA_SIGINFO;
	ss.sa_sigaction = aioreadhand;
	if(sigaction(SIGUSR1, &ss, NULL) == -1){
		cerr("Error setting up signal handler\n");
		exit(1);
	}

	struct aiocb ab;	
	ab.aio_fildes = udpsock;
	ab.aio_buf = mcd;
	ab.aio_nbytes= sizeof(struct maester_conn_data);
	ab.aio_offset=0;
	ab.aio_reqprio = 0;
	ab.aio_sigevent.sigev_notify = SIGEV_SIGNAL;
	ab.aio_sigevent.sigev_signo = SIGUSR1;
	int reqs = 0;
	while(1){
		sendto(udpsock, &pmesg, sizeof(uint64_t), 0, (struct sockaddr*)&sa, sizeof(sa));	// ping
		sleep(1);
		if(_pongrcved){
			if(mcd->magic == MMAGIC){
				printf("cancelling\n");
				aio_cancel(udpsock, NULL);
				while(reqs){
					int s = aio_error(&ab);
					if(s != EINPROGRESS)
						reqs--;
				}
				return;
			}
		}else{
			if(aio_read(&ab) == -1){
				cerr("Error with aio read\n");
				exit(1);
			}
			reqs++;
		}
	}
	return;
}

int send_string(char* buff, int len){
	struct msg_header mh;
	mh.type=MSG_STRING;
	mh.flags =0;
	mh.content_length = strlen(buff);
	if(ssend(maester_sock, &mh, sizeof(mh), 0) == -1) return -1;
	if(ssend(maester_sock, buff, mh.content_length, 0) == -1) return -1;
	return 0;
}

int maester_authorize(){
	char buff[64];
	int len;
	printf("Enter credential token: ");
	scanf("%s", buff);
	len = strlen(buff);
	if(send_string(buff, len) == -1){
		cerr("Cant send\n");
		return -1;
	}
	struct msg_header mh;
	if(srecv(maester_sock, &mh, sizeof(mh),0) == -1){
		cerr("Jared 19\n");
		return -1;
	}
	if(mh.type == MAESTER_OK) return 0;
	else return 1;
}

int send_my_info(){
	char hostname[HOST_NAME_MAX];
	struct conn_info ci;
	gethostname(ci.hostname, HOST_NAME_MAX);
	ci.type = 1;
	return ssend(maester_sock, &ci, sizeof(ci), 0);	
}

int find_and_connect_maester(){
	struct maester_conn_data mcd;
	await_connection(CLIENT_PORT, &mcd);
	struct sockaddr_in sa_maester;
	sa_maester.sin_family = AF_INET;
	inet_pton(AF_INET, mcd.addr, &(sa_maester.sin_addr));
	sa_maester.sin_port = htons(mcd.port);
	if((maester_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1){
		cerr("Error creating socket to maester\n");
		return -1;
	}
	int reuseenb=1;
	setsockopt(maester_sock, SOL_SOCKET, SO_REUSEADDR, &reuseenb, sizeof(reuseenb));
	if(connect(maester_sock, (struct sockaddr*) &sa_maester, sizeof(sa_maester)) < 0){
		cerr("Error. Cant connect to maester\n");
		return -1;
	}
	if(send_my_info() == -1){
		cerr("Error handshaking with maester server\n");
		return -1;
	}

	struct msg_header mh;
	if(srecv(maester_sock, &mh, sizeof(mh), 0) == -1){
		cerr("Error connecting to maester\n");
		return -1;
	}	
	if(mh.type == MAESTER_AUTH){
		return maester_authorize();
	}else if(mh.type == MAESTER_OK){
		return 0;
	}else if(mh.type == MAESTER_NOPE){
		return 1;
	}else{
		cerr("WHAT?!!\n");
		return -1;
	}
}

int send_job_header(int type, char* filename){
	struct msg_header header;
	header.type=CLIENT_ADD_JOB;
	header.flags = type;
	header.content_length= strlen(filename);
	ssend(maester_sock, &header, sizeof(header), 0);

	/*header.type=MSG_STRING;
	header.flags = 0;
	header.content_length= strlen(filename);
	ssend(maester_sock, &header, sizeof(header), 0);
	*/	
	ssend(maester_sock, filename, header.content_length, 0);

}

int send_exe_header(char* filename){
	struct msg_header header;
	header.type= MSG_STRING;
	header.flags = 0;
	header.content_length= strlen(filename);
	ssend(maester_sock, &header, sizeof(header), 0);
	
	ssend(maester_sock, filename, header.content_length, 0);
}
int send_file_content(FILE* file, off_t size){
	struct msg_header header;
	header.type=MSG_FILE;
	header.flags = 0;
	header.content_length= size;
	ssend(maester_sock, &header, sizeof(header), 0);

	int remaining_bytes = size;
	int read_bytes;
	char buff[DATA_BUFF_MAX];
	while(remaining_bytes > 0){
		read_bytes = fread(buff, sizeof(char), DATA_BUFF_MAX, file);
		if(read_bytes == 0) return -1;
		ssend(maester_sock, buff, read_bytes, 0);
		remaining_bytes -= read_bytes;
	}
	return 0;
}

int await_confirmation(struct maester_confirm* mc){
	struct msg_header mh;
	if(srecv(maester_sock, &mh, sizeof(mh), 0) == -1) return -1;
	if(mh.type != MAESTER_CONFIRM){
		cerr("SOMETHING IS TERRIBLY WRONG!\n");
		return -1;
	}
	if(srecv(maester_sock, mc, sizeof(struct maester_confirm), 0) == -1) return -1;
	return 0;
}

int send_list_header(){
	struct msg_header mh;
	mh.type = CLIENT_LIST_JOBS;
	mh.flags =0;
	mh.content_length = 0;
	return ssend(maester_sock, &mh, sizeof(mh),0);
}

char* statusstr(int st){
	switch(st){
		case JOB_STATUS_WAITING:
			return "waiting";
		case JOB_STATUS_RUNNING:
			return "running";
		case JOB_STATUS_DISCARDED:
			return "discarded";
		case JOB_STATUS_ENDED:
			return "ended";
		case JOB_STATUS_ORPHANED:
			return "orphaned";
		default:
			return "?";
	}
}

int get_and_print_jobs(){
	struct msg_header mh;
	struct job_list_entry jn;
	if(srecv(maester_sock, &mh, sizeof(mh),0));
	while(1){
		if(mh.type == MAESTER_NOPE){
			cerr("maester said no\n");
			return -1;
		}
		else if(mh.type == MAESTER_NEXT){
			if(srecv(maester_sock, &jn, sizeof(jn),0)){
				cerr("communication error");
				exit(1);
			}
			printf("%ld\t\t%s\t\t%s\n", jn.id, jn.workername, statusstr(jn.status));
			if(srecv(maester_sock, &mh, sizeof(mh),0));
		}else break;
	}
}

int send_log_header(int64_t id, uint16_t dir, uint32_t len){
	struct msg_header mh;
	mh.type = CLIENT_LOGS;
	mh.flags = id;
	mh.content_length = (dir ? 1: -1)*((int64_t)len);
	return ssend(maester_sock, &mh, sizeof(mh),0);
}

int send_top_header(){
	struct msg_header mh;
	mh.type=CLIENT_TOP;
	return ssend(maester_sock, &mh, sizeof(mh),0);
}

int main(int argc, char* argv[]){
	signal(SIGPIPE, SIG_IGN);
	if(argc == 1){
		print_usage(argv[0]);
		exit(0);
	}
	if(argc == 2){
		if(!strcmp(argv[1], "sh") || !strcmp(argv[1], "shell")){/*
			find_and_connect_maester();	
			char commandbuff[CMDBUFLEN];
			fgets(commandbuff, CMDBUFLEN, stdin);
			char opendelims[] = "\"\'{";
			char closdelims[] = "\"\'}";
			while(1){
				char* part = strmbtok(commandbuff, " ", opendelims, closdelims);
				if(!strcmp(part, "job")){

				}else if(!strcmp(part, "node")){

				}else if(!strcmp(part, "manage")){

				}else if(!strcmp(part, "exit")){
					break;
				}else{
					cerrf("Sorry cant understand %s\n", part);
					exit(1);
				}
			}
			disconnect_maester();*/
		}
	}else{
		if(!strcmp(argv[1], "job")){
			if(argc < 3){
				print_job_help(argv[0]);
				exit(1);
			}
			if(!strcmp(argv[2], "add")){
				if(argc<5){
					print_job_help(argv[0]);
					exit(1);
				}
				if(strcmp(argv[4], "-executable") && strcmp(argv[4], "-simple")){
					print_job_help(argv[0]);
					exit(1);
				}
				json_file_name = argv[3];
				FILE* json_file = fopen(json_file_name, "r");
				FILE* exefile;
				int isexe = 0;
				if(json_file == NULL){
					fprintf(stderr, "File %s not found.\n", json_file_name);
					exit(1);
				}
				struct stat stats;
				fstat(fileno(json_file), &stats);
				if(!strcmp(argv[4], "-executable")){
					isexe = 1;
					if(argc<6){
						print_job_help(argv[0]);
						exit(1);
					}
					executable_name = argv[5];
					exefile = fopen(executable_name, "r");
					if(exefile == NULL){
						cerrf("File %s is not found\n", executable_name);
						exit(1);
					}
				}
				int status;
				if((status = find_and_connect_maester()) != 0){
					if(status == -1) { cerr("couldnt connect to maester\n"); }
					else if(status == 1) { cerr("connection refused by maester\n"); }
					fclose(json_file);
					close(maester_sock);
					exit(1);
				}
				if(send_job_header(isexe, json_file_name) == -1){
					cerr("counldnt send message\n");
					fclose(json_file);
					close(maester_sock);
					exit(1);
				} // 0 for simple
				#ifdef _M_DEBUG_
				printf("sent job header\n");
				#endif
				if(send_file_content(json_file, stats.st_size) == -1){
					cerr("Failed sending json file\n");
					fclose(json_file);
					close(maester_sock);
					exit(1);
				}
				#ifdef _M_DEBUG_
				printf("sent json file\n");
				#endif
				if(isexe){
					fstat(fileno(exefile), &stats);
					if(send_exe_header(executable_name)){
						cerr("couldnt send message\n");
						fclose(json_file);
						fclose(exefile);
						close(maester_sock);
						exit(1);
					}
					send_file_content(exefile, stats.st_size);
					#ifdef _M_DEBUG_
					printf("sent exe fil\n");
					#endif
				}
				struct maester_confirm mc;
				while(1){
					if(await_confirmation(&mc) == -1){
						cerr("Communication error\n");
						exit(1);
					}
					if(mc.type == CONFIRM_JOB_QUEUED) printf("Job is queued.\n");
					else if(mc.type == CONFIRM_JOB_ADDED){
						printf("Job succeessfully added. Job id: %ld, worker id: %ld\n", mc.job_id, mc.worker_id);
						break;
					}else if(mc.type == CONFIRM_JOB_DISCARDED){
						printf("Job discarded by maester\n");
						break;
					}
				}
			}else if(!strcmp(argv[2], "list")){
				int status;
				if((status = find_and_connect_maester()) != 0){
					if(status == -1) { cerr("couldnt connect to maester\n"); }
					else if(status == 1) { cerr("connection refused by maester\n"); }
					exit(1);
				}
				if(send_list_header() == -1){
					cerr("counldnt send message\n");
					exit(1);
				} 
				#ifdef _M_DEBUG_
				printf("sent list header\n");
				#endif
				printf("JobID\t\tNode\t\tStatus\n---------------------------------------------------------\n");
				if(get_and_print_jobs()){
					cerr("Error\n");
					exit(1);
				}
			}else if(!strcmp(argv[2], "logs")){
				int status;
				printf("not implemented(ran out of time) :(\n");
				exit(1);
				if(argc<4){
					print_usage(argv[0]);
					exit(1);
				}
				int64_t jid = atoll(argv[3]);
				uint16_t dir = 0; // tail
				uint32_t len = 5;
				if(argc >= 5){
					if(!strcmp(argv[4], "-tail")) dir = 0;
					else if(!strcmp(argv[4], "-head")) dir =1;
					else {print_usage(argv[0]); exit(1);}
				}
				if(argc == 6){
					len = atol(argv[5]);
				}
				if((status = find_and_connect_maester()) != 0){
					if(status == -1) { cerr("couldnt connect to maester\n"); }
					else if(status == 1) { cerr("connection refused by maester\n"); }
					close(maester_sock);
					exit(1);
				}
				if(send_log_header(jid, dir, len)){
					cerr("error");
					exit(1);
				}
				struct msg_header mh;
				srecv(maester_sock, &mh, sizeof(mh),0);
				char* buff = malloc(mh.content_length);
				srecv(maester_sock, buff, mh.content_length,0);
				puts(buff);
				free(buff);

			}else{
				print_job_help(argv[0]);
				exit(1);
			} 
		}else if(!strcmp(argv[1], "node")){
			if(argc != 3){
				print_usage(argv[0]); exit(1);
			}
			if(!strcmp(argv[2], "top")){
				int status;
				if((status = find_and_connect_maester()) != 0){
					if(status == -1) { cerr("couldnt connect to maester\n"); }
					else if(status == 1) { cerr("connection refused by maester\n"); }
					close(maester_sock);
					exit(1);
				}
				if(send_top_header()){
					cerr("error");
					exit(1);
				}			
				struct msg_header mh;
				struct worker_top wt;
				srecv(maester_sock, &mh, sizeof(mh),0);
				printf("name\thostname\taddr\tcpu cnt\tmemtot\tmemfree\tjobs/maxjobs\n");
				while(mh.type != MSG_EMPTY){
					srecv(maester_sock, &wt, sizeof(wt),0);
					printf("%s\t%s\t%s\t%u\t%u\t%u\t%u/%u\n",
					wt.name, wt.hostname, wt.ipstr, 
					wt.cpucnt, wt.memtotal, wt.memfree, wt.jobs_cnt,
					wt.max_jobs);
				}
			}else{
				print_usage(argv[0]); exit(1);
			}

		}else if(!strcmp(argv[1], "manage")){
			find_and_connect_maester();	

		}else{
			cerrf("Sorry cant understand %s\n", argv[1]);
			exit(1);
		}
	}
	return 0;
}
