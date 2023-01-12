#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <stdint.h>

#include "common.h"

int ssend(int sockfd, void* buff, int len, int flags){
	int remaining_bytes = len;
	int sent_bytes =0;
	while(remaining_bytes){
		sent_bytes = send(sockfd, buff, remaining_bytes, flags);
		if(sent_bytes < 0) return -1;
		remaining_bytes -= sent_bytes;
		buff+= sent_bytes;
	}
	return 0;
}

int srecv(int sockfd, void* buff, int len, int flags){
	int remaining_bytes = len;
	int read_bytes;
	while(remaining_bytes){
		read_bytes = recv(sockfd, buff, remaining_bytes, flags);
		if(read_bytes < 0) return -1;
		if(read_bytes == 0) return -1; // assuming srecv is only called on blocking sockets. 
									   // only happen if the socket is closed by the peer
		remaining_bytes -= read_bytes;
		buff += remaining_bytes;
	}
	return 0;
}

