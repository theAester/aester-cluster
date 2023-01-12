#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <string.h>
#include <aio.h>
#include <arpa/inet.h>
#include <signal.h>
#include <getopt.h>
#include <limits.h>
#include <fcntl.h>
#include <time.h>

#include "common.h"
#include "logging.h"

#define BUFFSZ 2048

const char* appname; 

char homedir[DRSZ] = "./";

LIST_CREATE(struct job_node, job_list);

struct conn_info myinfo;
/////// configs
int _exitsigrecved =0;
static void siginthand(int sig){
	_exitsigrecved = 1;
	return;
}

void print_usage(){
	printf("Usage:\n%s -n name -d base-dir -l log-file -c config-file -p detection-port\n", appname);
	return;
}

void recv_maester_header(int sockfd, struct msg_header* mm){
	if(srecv(sockfd, mm, sizeof(struct msg_header),0) == -1){
		perr("Error receiving request headers from maester\n");
		exit(1);
	}
}

int configure_worker(char* homedir, char* configfilename){
	char tmp[DRSZ + NMSZ];
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
	myinfo.max_jobs = 10;
	sprintf(tmp, "%s%s", homedir, configfilename);
	fclose(file);
	file = fopen(tmp, "r");
	if(file == NULL){
		perrf("Warning, could not find config file at %s. Proceeding with default values\n", tmp);
		return 0;
	}
	while(fgets(tmp, 256, file) != NULL){
		if(!strncmp("max_jobs:", tmp, 9)){
			sscanf(tmp, "max_jobs: %d", &(myinfo.max_jobs));
		} // else if...
		else{
			perrf("Error cannot parse config file. Can't understand \'%s\'", tmp);
			fclose(file);
			return -1;
		}
	}	
	fclose(file);
	return 0;
}

int send_my_info(int sockfd){
	printf("type: %d\ncpucnt %d\nmemtotal %d\n memfree %d\n", myinfo.type, myinfo.cpucnt, myinfo.memtotal, myinfo.memfree);
	return ssend(sockfd, &myinfo, sizeof(myinfo), 0);	
}

int await_ack(int sockfd){
	struct msg_header mh;
	if(srecv(sockfd, &mh, sizeof(mh), 0) == -1) return -1;
	if(mh.type == MAESTER_OK) return 0;
	return -2;
}

int _pongrcved =0;

static void aioreadhand(int sig, siginfo_t* si, void* ctx){
	if(si->si_code == SI_ASYNCIO){
		printf("read!\n");
		_pongrcved = 1;
	}
}

void await_connection(int onport, struct maester_conn_data *mcd){
	int udpsock = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if(udpsock == -1){
		perr("Error creating socket\n");
		exit(1);
	}
	struct sockaddr_in sa_self;
	sa_self.sin_addr.s_addr = INADDR_ANY;
	sa_self.sin_port = htons(onport);
	sa_self.sin_family = AF_INET;
	if(bind(udpsock, (struct sockaddr*)&sa_self, sizeof(sa_self)) == -1){
		perr("Error binding socket\n");
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
		perr("Error setting up signal handler\n");
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
				perr("Error with aio read\n");
				exit(1);
			}
			reqs++;
		}
	}
	return;
}

int gmsock;

void sigchldhand(int sig){
	int pid;
	int status;
	struct job_node* ptr; 
	while(1){
		int ret = waitpid(-1, &status, WNOHANG);
		if(ret <= 0) break;
		ptr = job_list;
		while(ptr != NULL){
			if(ptr->tempfd != ret){
				ptr = ptr->next;
				continue;
			}
			ptr->exit_status = status;
			ptr->status = JOB_STATUS_ENDED;
			char* command = malloc((2*DRSZ + 2*NMSZ +128)*sizeof(char));
			time_t tm;
			time(&tm);
			if(WIFEXITED(status)){
			sprintf(command, "echo \">>>%s\\nJob ended normally\\nexit code: %d\\n\" | tee -a %sjob%ld/runtime.log.fifo >> %sjob%ld/runtime.log", ctime(&tm), WEXITSTATUS(status), homedir, ptr->id, homedir, ptr->id);
			}else{
			sprintf(command, "echo \">>>%s\\nJob ended abnormally\\n\" | tee -a %sjob%ld/runtime.log.fifo >> %sjob%ld/runtime.log", ctime(&tm), homedir, ptr->id, homedir, ptr->id);
			}
			system(command);
			
			//notify maester

			struct msg_header mh;
			mh.type = WORKER_JOB_DONE;
			mh.flags = ptr->id; 
			if(ssend(gmsock, &mh, sizeof(mh), 0)){
				perr("Error sending data to maester (in interrupt handler!!!)\n");
				close(gmsock);
			}
			break;
 		}
	}
	return;
}


int msystem(char* command){

}


int get_and_start_job(int socket, struct job_node* jn){
	srecv(socket, jn, sizeof(struct job_node), 0);
	char jdir[DRSZ+NMSZ];
	sprintf(jdir, "%sjob%ld/", homedir, jn->id);
	if(mkdir(jdir, 0777) == -1){
		perrf("Error creating directory for job %ld", jn->id);
		return -1;
	}
	char temp[DRSZ + 2*NMSZ];
	char fifoname[DRSZ + NMSZ + 16];
	sprintf(fifoname, "%sruntime.log.fifo", jdir);
	if(mkfifo(fifoname, 0666) < 0){
		perrf("Error creating fifo for job %ld\n", jn->id);
	}
	if(jn->type){
		temp[DRSZ + NMSZ];
		sprintf(temp, "%s%s", jdir, jn->exefilename);
		jn->tempfd = open(temp, O_RDWR | O_CREAT | O_TRUNC, 0777);
		uint32_t remaining_bytes = jn->exefilesize; 
		char buff[DATA_BUFF_MAX];
		int read_bytes;
		while(remaining_bytes > 0){
			if((read_bytes = recv(socket, buff, DATA_BUFF_MAX, 0)) == -1){
				perrf("Error communicating with maester while getting job %ld's executable\n", jn->id);
				return -1;
			}
			if(read_bytes == 0) continue;
			int written_bytes;
			while((written_bytes = write(jn->tempfd, buff, read_bytes)) != read_bytes) read_bytes -= written_bytes;
			remaining_bytes -= read_bytes;
		}
		close(jn->tempfd);
	}
	int childfd = fork();
	if(childfd){ // parent
		if(childfd<0){
			perrf("FORK ERROR! in creating job %ld\n", jn->id);
			return -1;
		}
		int fifofd = open(fifoname, O_RDONLY, 0666);
		if(fifofd < 0){
			perrf("Error cant open fifo in job %ld(parent process).\n", jn->id);
			return -1;
		}
		jn->fifofd = fifofd;
		jn->tempfd = childfd;
		return jn->id;
	}else{ // child
		chdir(jdir);
		sprintf(fifoname, "runtime.log.fifo");
		int fifofd = open(fifoname, O_WRONLY, 0666);
		if(fifofd < 0){
			perrf("Error cant open fifo in job %ld(child process).\n", jn->id);
			exit(1);
		}
		char command[CMDSZ + 128];
		time_t tm;
		time(&tm);
		int end_status;
		sprintf(command, "echo \">>>%s\\n[info] Starting job\\n\" | tee -a runtime.log.fifo >> runtime.log", ctime(&tm));
		system(command);
		sprintf(command, "bash -c \"%s\" 2>&1 | tee -a runtime.log.fifo >> runtime.log 2>&1", jn->ji.cmd);
		end_status = system(command);
		sprintf(command, "echo \">>>%s\\n[info] Starting job\\n\" | tee -a runtime.log.fifo >> runtime.log", ctime(&tm));
		exit(end_status);
	}
}

int main(int argc, char* argv[]){
	appname = argv[0];
	char logfilename[NMSZ] = "worker.log";
	char configfilename[NMSZ] = "worker.conf";
	char myname[NMSZ] = "new_worker";
	strcpy(myinfo.name, myname);
	gethostname(myinfo.hostname, HOST_NAME_MAX);
	int detport = 8585;
	int opt;
	myinfo.type=0;
	signal(SIGPIPE, SIG_IGN);
	while((opt = getopt(argc, argv, "hc:d:l:n:p:")) != -1){
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
	lprintf("\nstarting worker %s on %s\ndetecting master on port: %d\n", myinfo.name, myinfo.hostname, detport);
	printf("\nstarting worker %s on %s\ndetecting master on port: %d\n", myinfo.name, myinfo.hostname, detport);
	lprintf("initiated logfile at %s%s\n", homedir, logfilename);
	if(configure_worker(homedir, configfilename) == -1){
		perr("Error configuring worker\n");
		exit(1);
	}	
	lprintf("done configuring\n");
	lprintf("searching for maester\n");
	struct maester_conn_data msd;
	await_connection(detport, &msd);
	lprintf("found maester %s at %s\n", msd.maestername, msd.addr);
	printf("\t%s:%d\n", msd.addr, msd.port);
	struct sockaddr_in sa_maester;
	int maester_sock;
	sa_maester.sin_family = AF_INET;
	inet_pton(AF_INET, msd.addr, &(sa_maester.sin_addr));
	sa_maester.sin_port = htons(msd.port);
	if((maester_sock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) == -1){
		perr("Error in creating socket to maester\n");
		exit(1);
	}
	if(connect(maester_sock, (struct sockaddr*) &sa_maester, sizeof(sa_maester)) < 0){
		perr("Error. Cant connect to maester\n");
		exit(1);
	}
	lprintf("handshaking with maester server at %s\n", msd.addr);
	// TODO: authorization, skip that for now
	if(send_my_info(maester_sock) == -1){
		perrf("Error handshaking with maester server at %s\n", msd.addr);
		exit(1);
	}
	{
		int k;
		if((k = await_ack(maester_sock)) < 0){
			if(k == -1){ 
				perr("Acknowledgement cant be received\n");
			}else {
				perr("Wrong Acknowledgement message\n");
			}
			exit(1);
		}
	}
	lprintf("Successfully connected to maester server at %s", msd.addr);
	struct msg_header mm;
	uint8_t databuff[BUFFSZ];
	signal(SIGCHLD, sigchldhand);
	gmsock = maester_sock;
	while(! _exitsigrecved){
		printf("waiting\n"); // bruh
		recv_maester_header(maester_sock, &mm);
		printf("received header! %d", mm.type); // bruh
		switch(mm.type){
			case MAESTER_JOB:
				{
					struct job_node* jn = (struct job_node*)malloc(sizeof(struct job_node));
					int64_t jobid = get_and_start_job(maester_sock, jn);
					if(jobid == -1){
						perr("Error adding job\n");
					}
					lprintf("Job %ld started.\n", jobid);
					LIST_APPEND(job_list, jn);
					break;
				}
		}
		sleep(0.01);
	}
}
