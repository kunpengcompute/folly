#include <folly/io/async/urma/UrmaAsyncSocket.h>
#include <folly/io/async/urma/UrmaInit.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/IOBuf.h>
#include <array>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <vector>
using namespace folly;
struct Run;
struct Connection : AsyncTransport::ReadCallback, AsyncTransport::BufferCallback {
  struct Callback : AsyncTransport::WriteCallback {
    Connection* c; size_t index; unsigned calls{0};
    void writeSuccess() noexcept override;
    void writeErr(size_t, const AsyncSocketException&) noexcept override;
  };
  Run& run;
  AsyncTransport::UniquePtr socket;
  std::vector<std::string> storage;
  std::vector<Callback> callbacks;
  std::array<char, 65536> buffer;
  size_t received{0}, completed{0}, total{0}, expectedReceive{0}, errors{0};
  bool done{false};
  size_t bufferedEvents{0}, clearedEvents{0}, peakCapacity{0};
  void onEgressBuffered() override {
    auto bytes=socket->getAppBytesBuffered();
    auto capacity=socket->getAllocatedBytesBuffered();
    if((!bytes && !capacity) || capacity<bytes)std::abort();
    ++bufferedEvents;peakCapacity=std::max(peakCapacity,capacity);
    if(std::getenv("TEST_BUFFER_LIMIT") && capacity>std::stoull(std::getenv("TEST_BUFFER_LIMIT")))socket->closeNow();
  }
  void onEgressBufferCleared() override {
    if(socket->getAppBytesBuffered() || socket->getAllocatedBytesBuffered())std::abort();
    ++clearedEvents;
  }
  Connection(Run& r):run(r){}
  ~Connection(){ if(socket){static_cast<UrmaAsyncSocket*>(socket.get())->setBufferCallback(nullptr);socket->setReadCB(nullptr);socket->closeNow();} }
  void attach(AsyncTransport::UniquePtr);
  void receive(const char*, size_t);
  void progress();
  void fail(const std::string&);
  void getReadBuffer(void** p,size_t* n)override{*p=buffer.data();*n=buffer.size();}
  bool isBufferMovable()noexcept override;
  void readBufferAvailable(std::unique_ptr<IOBuf> p)noexcept override{
    auto* v=p.get(); do{receive(reinterpret_cast<const char*>(v->data()),v->length());v=v->next();}while(!done&&v!=p.get());
  }
  void readDataAvailable(size_t n)noexcept override{receive(buffer.data(),n);}
  void readEOF()noexcept override{if(!done){if(received==expectedReceive)progress();else fail("early EOF");}}
  void readErr(const AsyncSocketException& e)noexcept override{fail(e.what());}
};
struct Run {
  EventBase evb;
  bool server;
  size_t count, finished{0}, failed{0};
  std::vector<size_t> sizes;
  std::vector<std::unique_ptr<Connection>> connections;
  Run(bool s,size_t n):server(s),count(n){}
  void finish(bool ok){++finished;if(!ok)++failed;if(finished==count||!ok)evb.terminateLoopSoon();}
};
bool Connection::isBufferMovable()noexcept{return run.server;}
char pattern(size_t offset,bool server){return char((offset*7+(server?19:43))%251);}
void Connection::attach(AsyncTransport::UniquePtr p){
  socket=std::move(p);socket->setSendTimeout(std::getenv("TEST_TIMEOUT")?std::stoul(std::getenv("TEST_TIMEOUT")):12000);
  if(std::getenv("TEST_BUFFER_OBSERVER"))static_cast<UrmaAsyncSocket*>(socket.get())->setBufferCallback(this);
  storage.resize(run.sizes.size());callbacks.resize(run.sizes.size());
  for(size_t i=0;i<run.sizes.size();++i){
    storage[i].resize(run.sizes[i]);for(size_t j=0;j<storage[i].size();++j)storage[i][j]=pattern(total+j,run.server);
    total+=storage[i].size();callbacks[i].c=this;callbacks[i].index=i;
  }
  expectedReceive=total;
  if(std::getenv("TEST_ONE_WAY")){
    if(run.server){storage.clear();callbacks.clear();total=0;}
    else expectedReceive=0;
  }
  socket->setReadCB(this);
  for(size_t i=0;i<storage.size()&&!done;++i){
    auto& s=storage[i];std::vector<iovec> parts;size_t offset=0;
    for(size_t n:{size_t(0),size_t(7),size_t(13),size_t(0),size_t(113),size_t(3963)}){
      n=std::min(n,s.size()-offset);parts.push_back({s.data()+offset,n});offset+=n;
    }
    parts.push_back({s.data()+offset,s.size()-offset});parts.push_back({s.data()+s.size(),0});
    if (std::getenv("TEST_BATCH_RECORDS")) {
      std::vector<size_t> records;
      std::stringstream spec(std::getenv("TEST_BATCH_RECORDS"));
      std::string value;
      while (std::getline(spec,value,',')) records.push_back(std::stoull(value));
      auto chain=IOBuf::create(0);
      if (std::getenv("TEST_PART")) {
        size_t part=std::stoull(std::getenv("TEST_PART"));
        if (!part) part=s.size()+1;
        for(size_t pos=0;pos<s.size();pos+=part)
          chain->prependChain(IOBuf::copyBuffer(s.data()+pos,std::min(part,s.size()-pos)));
      } else {
        for(auto v:parts) chain->prependChain(IOBuf::copyBuffer(v.iov_base,v.iov_len));
      }
      dynamic_cast<UrmaAsyncSocket*>(socket.get())->writeChainWithRecords(
          &callbacks[i],std::move(chain),std::move(records));
    } else if(i%2)socket->writev(&callbacks[i],parts.data(),parts.size());
    else{auto chain=IOBuf::create(0);for(auto v:parts)chain->prependChain(IOBuf::copyBuffer(v.iov_base,v.iov_len));socket->writeChain(&callbacks[i],std::move(chain));}
  }
  if(std::getenv("TEST_CLOSE_MS"))run.evb.runAfterDelay([this]{if(!done)socket->closeNow();},std::stoi(std::getenv("TEST_CLOSE_MS")));
  if(std::getenv("TEST_HALF_CLOSE"))socket->shutdownWrite();
}
void Connection::receive(const char* p,size_t n){
  if(done)return;
  if(received+n>expectedReceive){fail("extra bytes");return;}
  for(size_t i=0;i<n;++i)if(p[i]!=pattern(received+i,!run.server)){fail("payload mismatch at "+std::to_string(received+i));return;}
  received+=n;
  if(std::getenv("TEST_DESTROY_ON_READ")){done=true;socket.reset();run.finish(false);return;}
  progress();
}
void Connection::Callback::writeSuccess()noexcept{
  if(++calls!=1)std::abort();
  if(c->done)return;
  if(index!=c->completed){c->fail("callback ordering");return;}
  if(std::getenv("TEST_DESTROY_ON_SUCCESS")){c->done=true;c->socket.reset();c->run.finish(false);return;}
  ++c->completed;c->progress();
}
void Connection::Callback::writeErr(size_t n,const AsyncSocketException& e)noexcept{
  if(++calls!=1)std::abort();
  ++c->errors;std::cerr<<"WRITE_ERROR index="<<index<<" completed="<<n<<'\n';c->fail(e.what());
}
void Connection::progress(){
  if(done||completed!=storage.size()||received!=expectedReceive)return;
  if(socket->getAppBytesWritten()!=total||socket->getAppBytesReceived()!=expectedReceive){fail("byte accounting");return;}
  done=true;socket->setReadCB(nullptr);socket->close();run.evb.runAfterDelay([this]{run.finish(true);},100);
}
void Connection::fail(const std::string& s){
  if(done)return;done=true;std::cerr<<"FAIL reason="<<s<<" rx="<<received<<" writes="<<completed<<'/'<<storage.size()<<'\n';
  socket->setReadCB(nullptr);socket->closeNow();run.finish(false);
}
struct Acceptor : AsyncServerSocket::AcceptCallback {
  Run& run;Acceptor(Run& r):run(r){}
  void connectionAccepted(NetworkSocket fd,const SocketAddress&,AcceptInfo)noexcept override{
    auto c=std::make_unique<Connection>(run);auto* p=c.get();run.connections.push_back(std::move(c));p->attach(AsyncTransport::UniquePtr(new UrmaAsyncSocket(&run.evb,fd.toFd())));
  }
  void acceptError(exception_wrapper e)noexcept override{std::cerr<<e.what()<<'\n';run.finish(false);}
  void acceptStarted()noexcept override{} void acceptStopped()noexcept override{}
};
int main(int argc,char**argv){
  std::cout.setf(std::ios::unitbuf);
  if(argc!=6)return 2;
  Run run(std::string(argv[1])=="server",std::stoul(argv[4]));
  std::stringstream ss(argv[5]);std::string item;while(std::getline(ss,item,','))run.sizes.push_back(std::stoul(item));
  try{
    UrmaConfig config;config.dev_name="udmac0d1e2";
    if(std::getenv("TEST_BUFFER"))config.buffer_size=std::stoul(std::getenv("TEST_BUFFER"));
    if(std::getenv("TEST_DEPTH"))config.rx_depth=config.tx_depth=std::stoul(std::getenv("TEST_DEPTH"));
    if(std::getenv("TEST_DISABLE_READ"))config.enable_read=false;
    if(std::getenv("TEST_THRESHOLD"))config.read_threshold=std::stoul(std::getenv("TEST_THRESHOLD"));
    if(std::getenv("TEST_PENDING"))config.max_pending_bytes=std::stoul(std::getenv("TEST_PENDING"));
    UrmaInit::init(config);
    auto listener=AsyncServerSocket::newSocket(&run.evb);Acceptor acceptor(run);
    if(run.server){listener->bind(SocketAddress(argv[2],std::stoul(argv[3])));listener->listen(128);listener->addAcceptCallback(&acceptor,&run.evb);listener->startAccepting();std::cout<<"LISTEN\n";}
    else for(size_t i=0;i<run.count;++i){auto c=std::make_unique<Connection>(run);auto* p=c.get();run.connections.push_back(std::move(c));p->attach(AsyncTransport::UniquePtr(new UrmaAsyncSocket(&run.evb,SocketAddress(argv[2],std::stoul(argv[3])))));}
    run.evb.runAfterDelay([&]{std::cerr<<"FAIL overall timeout\n";run.failed++;run.evb.terminateLoopSoon();},45000);
    if(run.finished<run.count)run.evb.loop();
    if(run.server)listener->stopAccepting();
    bool ok=run.finished==run.count&&!run.failed;
    size_t errors=0;for(auto& c:run.connections){
      errors+=c->errors;
      if(std::getenv("TEST_BUFFER_OBSERVER")) {
        if(ok && c->total && (!c->bufferedEvents || !c->clearedEvents))std::abort();
        if(c->socket && (c->socket->getAppBytesBuffered() || c->socket->getAllocatedBytesBuffered()))std::abort();
        std::cout<<"BUFFER changed="<<c->bufferedEvents<<" cleared="<<c->clearedEvents<<" peak="<<c->peakCapacity<<'\n';
      }
    }
    run.connections.clear();listener.reset();UrmaInit::fini();
    std::cout<<(ok?"PASS":"FAIL")<<" connections="<<run.finished<<'/'<<run.count<<" write_errors="<<errors<<" initialized="<<UrmaInit::isInitialized()<<'\n';
    return ok?0:1;
  }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}
}
