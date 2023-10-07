// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = 
    cl->call(extent_protocol::create, type, true, id);

  // std::cout << "create ret: " << ret << std::endl;
  
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = 
    cl->call(extent_protocol::get, eid, buf);

  // std::cout << "get ret: " << ret << std::endl;

  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = 
    cl->call(extent_protocol::getattr, eid, attr);

  // std::cout << "getattr ret: " << ret << std::endl;

  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf, bool iflog)
{ 
  int r;
  extent_protocol::status ret = 
    cl->call(extent_protocol::put, eid, buf, iflog, r);

  // std::cout << "put ret: " << ret << std::endl;

  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  int r;
  extent_protocol::status ret = 
    cl->call(extent_protocol::remove, eid, true, r);

  // std::cout << "remove ret: " << ret << std::endl;

  return ret;
}

extent_protocol::status 
extent_client::begin_tx()
{
  int r1,r2;
  extent_protocol::status ret = 
    cl->call(extent_protocol::begin_tx, r1, r2);

  // std::cout << "begin_tx ret: " << ret << std::endl;

  return ret;
}

extent_protocol::status 
extent_client::commit_tx()
{
  int r1,r2;
  extent_protocol::status ret = 
    cl->call(extent_protocol::commit_tx, r1, r2);

  // std::cout << "commit_tx ret: " << ret << std::endl;

  return ret;
}

extent_protocol::status 
extent_client::checkpoint()
{
  int r1,r2;
  extent_protocol::status ret = 
    cl->call(extent_protocol::checkpoint, r1, r2);

  // std::cout << "checkpoint ret: " << ret << std::endl;

  return ret;
} 

