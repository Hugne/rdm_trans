#include <sys/socket.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#include <assert.h>
#include <sys/queue.h>
#include <linux/tipc.h>
#include "common.h"

struct transaction {
	struct tipc_portid peer;
	int len;
	int tid;
	int expected_sid;
	char *buf;
	char *bufp;
	LIST_ENTRY(transaction) next;
};

LIST_HEAD(transaction_head, transaction) transaction_list =
	LIST_HEAD_INITIALIZER(transaction_list);

struct transaction* get_transaction(struct tipc_portid *pid, int tid)
{
	struct transaction *entry;
	LIST_FOREACH(entry, &transaction_list, next) {
		if ((pid->node == entry->peer.node) && (pid->ref == entry->peer.ref) &&
		    entry->tid == tid)
			return entry;
	}
	return NULL;
}

struct transaction* create_transaction(struct tipc_portid *pid, int len, int tid)
{
	struct transaction *tx = malloc(sizeof(struct transaction));
	memcpy(&tx->peer, pid, sizeof(struct tipc_portid));
	tx->len = len;
	tx->buf = malloc(len);
	tx->bufp = tx->buf;
	tx->expected_sid = 1;
	tx->tid = tid;
	LIST_INSERT_HEAD(&transaction_list, tx, next);
	//printf("transaction created\n");
	return tx;
}

void delete_transaction(struct transaction *tx) {
	LIST_REMOVE(tx, next);
	free(tx->buf);
	free(tx);
}
/*void delete_transaction(struct tipc_portid *pid)
{
	struct transaction *tx = get_transaction(pid);
	if (!tx) {
		perror("No such transaction\n");
		return;
	}
	LIST_REMOVE(tx, next);
	free(tx->buf);
	free(tx);
	//printf("transaction deleted\n");
}
*/

/*Returns true if transaction is complete, otherwise false*/
int transaction_process(int sd, struct transaction *tx, struct msghdr *msg,
			 size_t length)
{
	struct sockaddr_tipc *sa = msg->msg_name;
	struct header *hdr = msg->msg_iov[0].iov_base;
	char *data = msg->msg_iov[1].iov_base;

	/*Check that the TID of this segment matches the one for our
	 * ongoing transaction */
	if (tx->tid != hdr->tid) {
		printf("Transaction ID mismatch! dropping packet\n");
		return 0;
	}
	/*Check that the packet matches our expected sid,
	 * if this server's receivebuffer is overloaded, 
	 * messages might have been rejected and we get a
	 * sid gap*/
	if (tx->expected_sid != hdr->sid) {
	/*	
		printf("SID mismatch for transaction %d dropping packet\n",
			tx->tid);
		printf("Expected %d but received %d \n",
			tx->expected_sid, hdr->sid);
		printf("current buffer offset is 0x%x\n",
			(tx->buf - tx->bufp));
			*/
		return 0;
	}
	/*We got what we expected, copy the data and update the transaction*/
	memcpy(tx->bufp, data, length);
	tx->bufp += length;
	tx->expected_sid++;
	usleep(20);
	/*Check if we have received all expected data for the transaction*/
	if (tx->bufp == (tx->buf + tx->len)) {
		return 1;

	}
	return 0;
}

void acknowledge_transaction(int sd, struct transaction *tx)
{
	struct sockaddr_tipc sa;
	struct msghdr msg;
	struct iovec iov[1];
	struct header ack = {.tid = tx->tid, .length = tx->len};

	msghdr_create(&msg, iov, 1, NULL, 0, &sa);
	iov[0].iov_base = &ack;
	iov[0].iov_len = sizeof(ack);
	sa.family = AF_TIPC;
	sa.addrtype = TIPC_ADDR_ID;
	memcpy(&sa.addr.id, &tx->peer, sizeof(struct tipc_portid));
	if (sendmsg(sd, &msg, 0) <= 0)
		perror("sendto() failed to send ack\n");
}


int main(int argc, char *argv[])
{
	struct sockaddr_tipc sa_server = {
		.family = AF_TIPC,
		.addrtype = TIPC_ADDR_NAMESEQ,
		.addr.nameseq.type = SRV_TYPE,
		.addr.nameseq.lower = 0,
		.addr.nameseq.upper = 0,
		.scope = TIPC_ZONE_SCOPE };
	struct sockaddr_tipc sa_peer;
	socklen_t sa_len;
	unsigned int txlen;
	size_t nbytes;
	char *mfrag;
	int sd;
	struct transaction *tx;
	struct header hdr;
	struct msghdr msg;
	struct iovec iov[2];

	mfrag = malloc(TIPC_MAX_USER_MSG_SIZE);
	msg.msg_iov = iov;
	msg.msg_iovlen = 2;
	msg.msg_name = &sa_peer;
	msg.msg_namelen = sizeof(sa_peer);
	msg.msg_control = NULL;
	msg.msg_controllen = 0;
	iov[0].iov_base = &hdr;
	iov[0].iov_len = sizeof(hdr);
	iov[1].iov_base = mfrag;
	iov[1].iov_len = TIPC_MAX_USER_MSG_SIZE;

	sd = socket(AF_TIPC, SOCK_RDM, 0);
	if (bind(sd, (struct sockaddr*) &sa_server, sizeof(sa_server)) != 0) {
		perror("bind()");
		return EXIT_FAILURE;
	}

	while (1)
	{
		/*Debug delay*/
		usleep(1000);
		sa_len = sizeof(sa_peer);
		nbytes = recvmsg(sd, &msg, 0);
		if (nbytes <= 0)
		{
			perror("recvmsg()");
			free(mfrag);
			return EXIT_FAILURE;
		}
	//	printf("read %d bytes, header fields: type = 0x%x length=%d\n",
	//		nbytes, header.type, header.length);

		/*Check if we have an ongoing transaction from the remote TIPC portid */
		if ((tx = get_transaction(&sa_peer.addr.id, hdr.tid)) == NULL) {
			/*If not, the segment id should be 1, indicating
			 * beginning of a transaction */
			if (hdr.sid == 1) {
				/*Create a new transaction for the peer*/
				tx = create_transaction(&sa_peer.addr.id,
							hdr.length,
							hdr.tid);
				if (!tx) {
					perror("could not create transaction\n");
					close(sd);
					free(mfrag);
					return EXIT_FAILURE;
				}
				/*
				printf("Transaction from ");
				print_portid(&sa_peer.addr.id);
				printf(" created\n");
				*/
			}
			else {
				//printf("Transaction RX error, sid is %d should be 1\n",hdr.sid);
				continue;
			}
		}
		/*Handle message for an ongoing (or a newly created) transaction*/
		if(transaction_process(sd, tx, &msg, (nbytes - sizeof(hdr))))
		{
			printf("Transaction %d (%d bytes) complete!\n",
					tx->tid, tx->len);
			acknowledge_transaction(sd, tx);
			delete_transaction(tx);
		}
	}

	close(sd);
	free(mfrag);
	return EXIT_SUCCESS;
}
