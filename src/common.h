#ifndef __TRANSACTION_H__
#define __TRANSACTION_H__

#define SRV_TYPE 1234

#define TX_BUFSIZE (2<<24) /* 16MB transaction buffer */

struct header {
	uint32_t length;	/*Length of complete transaction*/
	uint32_t tid;		/*Transaction ID*/
	uint32_t sid;		/*Segment ID*/
};
/* payload data follows */

inline void print_portid(struct tipc_portid *pid)
{
	printf("<%d.%d.%d:%u>",
		tipc_zone(pid->node),
		tipc_cluster(pid->node), 
		tipc_node(pid->node),
		pid->ref);
}


/*Create a simple msghdr with header and data iovs,
  and a buffer for ancillary data */
void msghdr_create(struct msghdr* m, struct iovec* iov,
		   int iovlen, void* control_buf, int control_len,
                   struct sockaddr_tipc* saddr)
{
        memset(m,0,sizeof(struct msghdr));
        memset(iov,0,sizeof(struct iovec) * iovlen);
        m->msg_iov = iov;
        m->msg_iovlen = iovlen;
        m->msg_name = saddr;
        m->msg_namelen = sizeof(struct sockaddr_tipc);
        m->msg_control = control_buf;
        m->msg_controllen = control_len;
}

#define CMSG_TIPC_ERRINFO(msg) ((int*)get_ancillary_attr(msg, SOL_TIPC, TIPC_ERRINFO))
#define CMSG_TIPC_RETDATA(msg) ((struct header*)get_ancillary_attr(msg, SOL_TIPC, TIPC_RETDATA))
void* get_ancillary_attr(struct msghdr* msg, int cmsg_lvl, int cmsg_type)
{
	struct cmsghdr *cmsg;
	cmsg = CMSG_FIRSTHDR(msg);

	while(cmsg != NULL)
	{
		if ((cmsg->cmsg_level == cmsg_lvl) &&
			(cmsg->cmsg_type == cmsg_type))
		{
			return (void*)CMSG_DATA(cmsg);
		}
		cmsg = CMSG_NXTHDR(msg, cmsg);
	}
	return NULL;
}
#endif
