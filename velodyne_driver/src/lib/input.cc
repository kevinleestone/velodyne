/*
 *  Copyright (C) 2007 Austin Robot Technology, Patrick Beeson
 *  Copyright (C) 2009, 2010 Austin Robot Technology, Jack O'Quin
 *  Copyright (C) 2015, Jack O'Quin
 *
 *  License: Modified BSD Software License Agreement
 *
 *  $Id$
 */

/** \file
 *
 *  Input classes for the Velodyne HDL-64E 3D LIDAR:
 *
 *     Input -- base class used to access the data independently of
 *              its source
 *
 *     InputSocket -- derived class reads live data from the device
 *              via a UDP socket
 *
 *     InputPCAP -- derived class provides a similar interface from a
 *              PCAP dump
 */

#include <unistd.h>
#include <string>
#include <sstream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <poll.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <velodyne_driver/input.h>


#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include <time.h>
#include <getopt.h>

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>

#include <linux/net_tstamp.h>
#include <linux/sockios.h>


#define TRY(x)                                                          \
  do {                                                                  \
    int __rc = (x);                                                     \
      if( __rc < 0 ) {                                                  \
        fprintf(stderr, "ERROR: TRY(%s) failed\n", #x);                 \
        fprintf(stderr, "ERROR: at %s:%d\n", __FILE__, __LINE__);       \
        fprintf(stderr, "ERROR: rc=%d errno=%d (%s)\n",                 \
                __rc, errno, strerror(errno));                          \
        exit(1);                                                        \
      }                                                                 \
  } while( 0 )


namespace velodyne_driver
{

  static const size_t packet_size =
    sizeof(velodyne_msgs::VelodynePacket().data);

  ////////////////////////////////////////////////////////////////////////
  // Input base class implementation
  ////////////////////////////////////////////////////////////////////////

  /** @brief constructor
   *
   *  @param private_nh ROS private handle for calling node.
   *  @param port UDP port number.
   */
  Input::Input(ros::NodeHandle private_nh, uint16_t port):
    private_nh_(private_nh),
    port_(port)
  {
    private_nh.param("device_ip", devip_str_, std::string(""));
    if (!devip_str_.empty())
      ROS_INFO_STREAM("Only accepting packets from IP address: "
                      << devip_str_);
  }

  ////////////////////////////////////////////////////////////////////////
  // InputSocket class implementation
  ////////////////////////////////////////////////////////////////////////

  /** @brief constructor
   *
   *  @param private_nh ROS private handle for calling node.
   *  @param port UDP port number
   */
  InputSocket::InputSocket(ros::NodeHandle private_nh, uint16_t port):
    Input(private_nh, port)
  {
    sockfd_ = -1;
    
    if (!devip_str_.empty()) {
      inet_aton(devip_str_.c_str(),&devip_);
    }    

    // connect to Velodyne UDP port
    ROS_INFO_STREAM("Opening UDP socket: port " << port);
    sockfd_ = socket(PF_INET, SOCK_DGRAM, 0);
    if (sockfd_ == -1)
      {
        perror("socket");               // TODO: ROS_ERROR errno
        return;
      }
  
    sockaddr_in my_addr;                     // my address information
    memset(&my_addr, 0, sizeof(my_addr));    // initialize to zeros
    my_addr.sin_family = AF_INET;            // host byte order
    my_addr.sin_port = htons(port);          // port in network byte order
    my_addr.sin_addr.s_addr = INADDR_ANY;    // automatically fill in my IP
  
    if (bind(sockfd_, (sockaddr *)&my_addr, sizeof(sockaddr)) == -1)
      {
        perror("bind");                 // TODO: ROS_ERROR errno
        return;
      }
  
    if (fcntl(sockfd_,F_SETFL, O_NONBLOCK|FASYNC) < 0)
      {
        perror("non-block");
        return;
      }

    ROS_DEBUG("Velodyne socket fd is %d\n", sockfd_);

    int enable = 1;

    // **************************************************************************
    // New timestamping
    ROS_DEBUG("Selecting hardware timestamping mode.");
    enable = SOF_TIMESTAMPING_RX_HARDWARE | SOF_TIMESTAMPING_RAW_HARDWARE
              | SOF_TIMESTAMPING_SYS_HARDWARE | SOF_TIMESTAMPING_SOFTWARE;
    TRY(setsockopt(sockfd_, SOL_SOCKET, SO_TIMESTAMPING, &enable, sizeof(int)));
    
  }

  /** @brief destructor */
  InputSocket::~InputSocket(void)
  {
    (void) close(sockfd_);
  }

  /** @brief Get one velodyne packet. */
  int InputSocket::getPacket(velodyne_msgs::VelodynePacket *pkt, const double time_offset)
  {
    double time1 = ros::Time::now().toSec();

    struct pollfd fds[1];
    fds[0].fd = sockfd_;
    fds[0].events = POLLIN;
    static const int POLL_TIMEOUT = 1000; // one second (in msec)

    sockaddr_in sender_address;
    socklen_t sender_address_len = sizeof(sender_address);

    // **************************************************************************
    // New timestamping
    struct msghdr msg;
    struct iovec iov;
    char control[1024];
  
    bzero(&sender_address, sizeof(struct sockaddr_in));
  
    iov.iov_base = &pkt->data[0];
    iov.iov_len = packet_size;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_name = &sender_address;
    msg.msg_namelen = sizeof(struct sockaddr_in);
    msg.msg_control = control;
    msg.msg_controllen = 1024;
  
    
    while (true)
      {
        // Unfortunately, the Linux kernel recvfrom() implementation
        // uses a non-interruptible sleep() when waiting for data,
        // which would cause this method to hang if the device is not
        // providing data.  We poll() the device first to make sure
        // the recvfrom() will not block.
        //
        // Note, however, that there is a known Linux kernel bug:
        //
        //   Under Linux, select() may report a socket file descriptor
        //   as "ready for reading", while nevertheless a subsequent
        //   read blocks.  This could for example happen when data has
        //   arrived but upon examination has wrong checksum and is
        //   discarded.  There may be other circumstances in which a
        //   file descriptor is spuriously reported as ready.  Thus it
        //   may be safer to use O_NONBLOCK on sockets that should not
        //   block.

        // poll() until input available
        do
          {
            int retval = poll(fds, 1, POLL_TIMEOUT);
            if (retval < 0)             // poll() error?
              {
                if (errno != EINTR)
                  ROS_ERROR("poll() error: %s", strerror(errno));
                return 1;
              }
            if (retval == 0)            // poll() timeout?
              {
                ROS_WARN("Velodyne poll() timeout");
                return 1;
              }
            if ((fds[0].revents & POLLERR)
                || (fds[0].revents & POLLHUP)
                || (fds[0].revents & POLLNVAL)) // device error?
              {
                ROS_ERROR("poll() reports Velodyne error");
                return 1;
              }
          } while ((fds[0].revents & POLLIN) == 0);

        // Receive packets that should now be available from the
        // socket using a blocking read.
        //ssize_t nbytes = recvfrom(sockfd_, &pkt->data[0],
        //                          packet_size,  0,
        //                          (sockaddr*) &sender_address,
        //                          &sender_address_len);
        ssize_t nbytes = recvmsg(sockfd_, &msg, 0);

        if (nbytes < 0)
          {
            if (errno != EWOULDBLOCK)
              {
                perror("recvfail");
                ROS_INFO("recvfail");
                return 1;
              }
          }
        else if ((size_t) nbytes == packet_size)
          {
            // read successful,
            // if packet is not from the lidar scanner we selected by IP,
            // continue otherwise we are done
            if(devip_str_ != ""
               && sender_address.sin_addr.s_addr != devip_.s_addr)
              continue;
            else
              break; //done
          }

        ROS_DEBUG_STREAM("incomplete Velodyne packet read: "
                         << nbytes << " bytes");
      }

    pkt->stamp = handleTime(&msg);

    return 0;
  }

  // Extract timestamp from packet metadata fallback to using system time.
  ros::Time InputSocket::handleTime(struct msghdr* msg)
  {
    struct timespec* ts = NULL;
    struct cmsghdr* cmsg;
    ros::Time stamp;
  
    for( cmsg = CMSG_FIRSTHDR(msg); cmsg; cmsg = CMSG_NXTHDR(msg,cmsg) ) {
      if( cmsg->cmsg_level != SOL_SOCKET )
        continue;
  
      switch( cmsg->cmsg_type ) {
      case SO_TIMESTAMPNS:
        ts = (struct timespec*) CMSG_DATA(cmsg);
        break;
      case SO_TIMESTAMPING:
        ts = (struct timespec*) CMSG_DATA(cmsg);
        break;
      default:
        /* Ignore other cmsg options */
        break;
      }
    }

    if( ts != NULL ) {
      // Use first of three timestamps, system time based
      stamp = ros::Time(ts[0].tv_sec, ts[0].tv_nsec);

      ROS_INFO_ONCE("Using packet metadata to get time (best)");
    } else {
      // Timestamp not available from linux socket.
      //stamp = ros::Time::now();

      // FIXME: remove this before checking in
      stamp = ros::Time(); 

      ROS_WARN_ONCE("Using syscall to get time (nominal)");
    }

    return stamp;
  }

  ////////////////////////////////////////////////////////////////////////
  // InputPCAP class implementation
  ////////////////////////////////////////////////////////////////////////

  /** @brief constructor
   *
   *  @param private_nh ROS private handle for calling node.
   *  @param port UDP port number
   *  @param packet_rate expected device packet frequency (Hz)
   *  @param filename PCAP dump file name
   */
  InputPCAP::InputPCAP(ros::NodeHandle private_nh, uint16_t port,
                       double packet_rate, std::string filename,
                       bool read_once, bool read_fast, double repeat_delay):
    Input(private_nh, port),
    packet_rate_(packet_rate),
    filename_(filename)
  {
    pcap_ = NULL;  
    empty_ = true;

    // get parameters using private node handle
    private_nh.param("read_once", read_once_, false);
    private_nh.param("read_fast", read_fast_, false);
    private_nh.param("repeat_delay", repeat_delay_, 0.0);

    if (read_once_)
      ROS_INFO("Read input file only once.");
    if (read_fast_)
      ROS_INFO("Read input file as quickly as possible.");
    if (repeat_delay_ > 0.0)
      ROS_INFO("Delay %.3f seconds before repeating input file.",
               repeat_delay_);

    // Open the PCAP dump file
    ROS_INFO("Opening PCAP file \"%s\"", filename_.c_str());
    if ((pcap_ = pcap_open_offline(filename_.c_str(), errbuf_) ) == NULL)
      {
        ROS_FATAL("Error opening Velodyne socket dump file.");
        return;
      }

    std::stringstream filter;
    if( devip_str_ != "" )              // using specific IP?
      {
        filter << "src host " << devip_str_ << " && ";
      }
    filter << "udp dst port " << port;
    pcap_compile(pcap_, &pcap_packet_filter_,
                 filter.str().c_str(), 1, PCAP_NETMASK_UNKNOWN);
  }

  /** destructor */
  InputPCAP::~InputPCAP(void)
  {
    pcap_close(pcap_);
  }

  /** @brief Get one velodyne packet. */
  int InputPCAP::getPacket(velodyne_msgs::VelodynePacket *pkt, const double time_offset)
  {
    struct pcap_pkthdr *header;
    const u_char *pkt_data;

    while (true)
      {
        int res;
        if ((res = pcap_next_ex(pcap_, &header, &pkt_data)) >= 0)
          {
            // Skip packets not for the correct port and from the
            // selected IP address.
            if (!devip_str_.empty() &&
                (0 == pcap_offline_filter(&pcap_packet_filter_,
                                          header, pkt_data)))
              continue;

            // Keep the reader from blowing through the file.
            if (read_fast_ == false)
              packet_rate_.sleep();
            
            memcpy(&pkt->data[0], pkt_data+42, packet_size);
            pkt->stamp = ros::Time::now(); // time_offset not considered here, as no synchronization required
            empty_ = false;
            return 0;                   // success
          }

        if (empty_)                 // no data in file?
          {
            ROS_WARN("Error %d reading Velodyne packet: %s", 
                     res, pcap_geterr(pcap_));
            return -1;
          }

        if (read_once_)
          {
            ROS_INFO("end of file reached -- done reading.");
            return -1;
          }
        
        if (repeat_delay_ > 0.0)
          {
            ROS_INFO("end of file reached -- delaying %.3f seconds.",
                     repeat_delay_);
            usleep(rint(repeat_delay_ * 1000000.0));
          }

        ROS_DEBUG("replaying Velodyne dump file");

        // I can't figure out how to rewind the file, because it
        // starts with some kind of header.  So, close the file
        // and reopen it with pcap.
        pcap_close(pcap_);
        pcap_ = pcap_open_offline(filename_.c_str(), errbuf_);
        empty_ = true;              // maybe the file disappeared?
      } // loop back and try again
  }


} // velodyne namespace
