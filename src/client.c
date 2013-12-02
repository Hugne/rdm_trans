#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <assert.h>
#include <linux/tipc.h>

#include "common.h"

#define CLIENT_LOG(...) {printf("Client %d: ", getpid()); printf(__VA_ARGS__); }
static int txid = 0;

/* Segment and send a long message to a RDM server
 *
 * */
int transaction_send(int sd, struct sockaddr_tipc *sa_remote,
					 char *buf, int bufsize, int segsize)
{
	CLIENT_LOG("segment and send bufsize=%d segsize=%d nsegs=%d\n",
					bufsize, segsize, bufsize/segsize);
	char *bufp = buf;
	struct msghdr msg;
	struct iovec iov[2];
	size_t nsent = 0;
	size_t nbytes;
	struct header hdr;
	int sid = 1;
	int reject;
	int delay_us = 0;
	int i;
	int nrejected = 0;
	int last_acked = 0;

	msghdr_create(&msg, iov, 2, NULL, 0, sa_remote);
	iov[0].iov_base = &hdr;
	iov[0].iov_len = sizeof(hdr);
	nbytes = (bufsize < segsize ? bufsize : segsize);
	hdr.length = bufsize;
	hdr.tid = txid++;
again:
	while(bufp < (buf + bufsize)) {
		iov[1].iov_base = bufp;
		iov[1].iov_len = nbytes;
		hdr.sid = sid++;
		/*TODO: we should establish the unique portid of the
		 * serving entity that received the first message here.
		 * All subsequent messages should be sent to that
		 * specific instance*/
		nsent = sendmsg(sd, &msg, 0);
		if (nsent <= 0) {
			perror("sendto()");
			close(sd);
			free(buf);
			return EXIT_FAILURE;
		}
		if (last_acked+WINSIZE <= sid)	/*Block, w4ack*/
			reject = transaction_acknowledge(sd, &last_acked, 0);
		else
			reject = transaction_acknowledge(sd, &last_acked, MSG_DONTWAIT);
		if (reject > 0 && reject <= sid) {
			i=1;
			//CLIENT_LOG("server rejected message %d while sending %d\n", reject, sid);
			sid = reject;
			bufp = buf;
			/*Speculatively reassign nbytes*/
			nbytes =  (bufsize < segsize ? bufsize : segsize);
			/*Rewind bufp to the rejected msg offset*/
			bufp = buf+(segsize*(sid-1));
			if (bufp + nbytes > buf + bufsize) {
				nbytes = (buf+bufsize)-bufp;
				//CLIENT_LOG("Rewinded last packet (sid=%d) set nbytes to %d bufp=%p buf=%p bufsize=0x%x\n", sid, nbytes, bufp, buf, bufsize);
			}
			nrejected++;
			/* hardcoded grace period after reject
			 * When we receive a rejected message due to overload, 
			 * there is no point to keep pushing.
			 * Bumping the importance level would reduce the amount
			 * of overloads received, but this potentially leads
			 * to a complete starvation for other senders.
			 *
			 * The grace could be replaced with a ACK mechanism that
			 * acknowledges every 10 packets (and always the last one)
			 * Transmission is only allowed to continue if we have
			 * an open slot in this window.*/
			usleep(1000);
			goto again;
		}
		bufp += nsent - sizeof(hdr);
		if ((long)(bufp + nbytes) > (long)(buf + bufsize)) {
			nbytes = (buf+bufsize)-bufp;
			//CLIENT_LOG("Send last message of %d nbytes bufp=%p buf=%p bufsize=0x%x\n", nbytes, bufp, buf, bufsize);
		}
	}
	CLIENT_LOG("All sent, check for acks\n");
	/*All data sent, now check for ack or reject */
	if ((sid = transaction_acknowledge(sd, &last_acked, 0))) {
		nrejected++;
		//CLIENT_LOG("server rejected message %d\n", sid);
		/*Speculatively reassign nbytes*/
		nbytes =  (bufsize < segsize ? bufsize : segsize);
		bufp = buf + ((sid-1)*segsize);
		//CLIENT_LOG("restart transaction with sid %d at offset 0x%x\n",sid, buf-bufp);
		if ((bufp + nbytes) > (buf + bufsize)) {
			nbytes = (buf+bufsize)-bufp;
			//CLIENT_LOG("Rewinded last packet (sid=%d) set nbytes to %d bufp=%p buf=%p bufsize=0x%x\n", sid, nbytes, bufp, buf, bufsize);
		}
		goto again;
	}
	/*All data acked by server*/
	printf("Transaction completed with %d rejects\n", nrejected);
	return bufsize;
}

/* returns 0 if server have acknowledged all messages that we sent so far,
 * or in the case that we got a nak/reject the segment ID of the
 * first rejected message
 */
int transaction_acknowledge(int sd, int *last_acked, int flags)
{
		struct sockaddr_tipc sa_remote; 
		struct msghdr msg;
		struct iovec iov[1];
		struct header ack;
		char *cbuf[1024];
		int *err = NULL;
		struct header *rejhdr;
		int res;


		msghdr_create(&msg, iov, 1, cbuf, 1024, &sa_remote);
		iov[0].iov_base = &ack;
		iov[0].iov_len = sizeof(ack);

		res = recvmsg(sd, &msg, flags);
		if (res < 0) {
			if (errno == EAGAIN) {
					/*No rejects/acks */
					return -1;
			}
			/*Fault*/
			perror("recvmsg()");
			exit (EXIT_FAILURE);
		}
		else if (res == 0) {
			/*Nothing acked, something rejected?*/
			err = CMSG_TIPC_ERRINFO(&msg);
			if (err) {
				//CLIENT_LOG("received rejected message with errno %d\n", *err);
				rejhdr = CMSG_TIPC_RETDATA(&msg);
				//CLIENT_LOG("rejected message in transaction %d with segment id %d\n",
				//				rejhdr->tid, rejhdr->sid);
				int sid = rejhdr->sid;
				return sid;
			}
			return 0;
		} else {
			/*We got an ack*/
			CLIENT_LOG("Server acked message %d\n", ack.sid);
			*last_acked = ack.sid;
			return 0;
		}
		return -1;
}

int main(int argc, char *argv[])
{
	struct sockaddr_tipc sa_remote = {
			.family = AF_TIPC,
			.addrtype = TIPC_ADDR_NAME,
			.addr.name.name.type = SRV_TYPE,
			.addr.name.name.instance = 0,
			.addr.name.domain = 0 };
	size_t bufsize = 65535;
	size_t segsize = 512;
	int c;
	int nclients = 1;
	int iterations = 1;
	int sid;
	int i = 0;
	while ((c = getopt(argc, argv, "hn:i:t:s:")) != -1)
	{
			switch (c) {
			case 'n':
					nclients = atoi(optarg);
			break;
			case 'i':
				iterations = atoi(optarg);
				break;
			case 't':
				bufsize = atoi(optarg);
				break;
			case 's':
				segsize = atoi(optarg);
				break;
			case 'h':
			default:
				printf("Usage: %s\n", argv[0]);
				printf("-n <number of clients>\n");
				printf("-i <iterations>\n");
				printf("-t <bufsize in bytes>\n");
				printf("-s <segment size in bytes>\n");
				return -1;
			break;
			}
	}
	printf("Running %d iterations on %d clients\n", iterations, nclients);
	printf("Transaction size = %d bytes\n", (int) bufsize);
	printf("Segment size=%d bytes\n", (int) segsize);

	srand(time(NULL));
	
	while (i++ < nclients) {
			CLIENT_LOG("Spawned %d\n", i);
			if (!fork()) {
				int sd;
				char *buf = malloc(bufsize);
				int k = iterations;

				if (!buf) {
					perror("malloc()");
					return EXIT_FAILURE;
				}

				sd = socket(AF_TIPC, SOCK_RDM, 0);
				static const int off = 0;
				if (setsockopt(sd, SOL_TIPC, TIPC_DEST_DROPPABLE, &off, sizeof(off)) != 0) {
						perror("setsockopt");
						return EXIT_FAILURE;
				}
				while (k-- > 0)
				{
					CLIENT_LOG("iteration %d\n",iterations);
					if (!transaction_send(sd, &sa_remote, buf, bufsize, segsize)) {
						CLIENT_LOG("transaction_send error");
						close(sd);
						free(buf);
						return EXIT_FAILURE;
					}
				}
				CLIENT_LOG("Finished transmission successfully\n");
				close(sd);
				free(buf);
				return EXIT_SUCCESS; /**/
			}
	}
	pid_t wpid;
	int wstatus;
	while ((wpid = wait(&wstatus)) > 0)
	{
		printf("Exit status of %d was %d (%s)\n", (int)wpid, wstatus,
			(wstatus >= 0) ? "OK" : "FAIL");
	}
	return EXIT_SUCCESS;
}
















