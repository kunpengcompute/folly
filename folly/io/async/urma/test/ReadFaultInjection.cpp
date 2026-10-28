#include <urma_api.h>
#include <dlfcn.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <chrono>
#include <unordered_map>
#include <deque>
namespace {
std::atomic<unsigned long> sends{0},offers{0},acks{0},reads{0},readBytes{0},registered{0},unregistered{0},imports{0},unimports{0},injected{0},rnr{0};
std::atomic<unsigned long> regUs{0},unregUs{0},importUs{0},unimportUs{0};
using Clock=std::chrono::steady_clock;
unsigned long us(Clock::time_point start){return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now()-start).count();}
std::atomic<int> events{0};
void event(const char* name,unsigned long id,unsigned long value){
  if(std::getenv("READ_TIMELINE")&&events++<160)std::fprintf(stderr,"EVENT %lu %s id=%lu value=%lu\n",us(Clock::time_point{}),name,id,value);
}
std::mutex mu;
std::unordered_map<urma_jfc_t*,std::deque<urma_cr_t>> fake;
std::unordered_map<urma_jfc_t*,std::pair<Clock::time_point,urma_cr_t>> held;
std::unordered_map<urma_jfc_t*,uint64_t> lastOffer;
std::unordered_map<urma_target_seg_t*,uint32_t> sourceAttrs;
const char* fault(){auto p=std::getenv("READ_FAULT");return p?p:"";}
bool once(const char* mode){return !std::strcmp(fault(),mode)&&!injected.exchange(1);}
template<class T>T next(const char* n){return reinterpret_cast<T>(dlsym(RTLD_NEXT,n));}
__attribute__((destructor))void report(){
  std::fprintf(stderr,"TRACE sends=%lu offers=%lu acks=%lu reads=%lu read_bytes=%lu reg=%lu unreg=%lu imports=%lu unimports=%lu injected=%lu rnr=%lu\n",sends.load(),offers.load(),acks.load(),reads.load(),readBytes.load(),registered.load(),unregistered.load(),imports.load(),unimports.load(),injected.load(),rnr.load());
  std::fprintf(stderr,"SDK_TIME reg_us=%lu unreg_us=%lu import_us=%lu unimport_us=%lu\n",regUs.load(),unregUs.load(),importUs.load(),unimportUs.load());
}
}
extern "C" urma_target_seg_t* urma_register_seg(urma_context_t* c,urma_seg_cfg_t* s){
  if(s->flag.bs.access==URMA_ACCESS_READ&&once("source-reg")){errno=ENOMEM;return nullptr;}
  if(s->flag.bs.access==URMA_ACCESS_LOCAL_ONLY&&s->len==4096&&once("dest-reg")){errno=ENOMEM;return nullptr;}
  auto start=Clock::now();auto p=next<decltype(&urma_register_seg)>("urma_register_seg")(c,s);regUs+=us(start);event("reg",s->len,us(start));if(p)++registered;
  if(p&&s->flag.bs.access==URMA_ACCESS_READ&&once("source-attr")){
    std::lock_guard<std::mutex> l(mu);sourceAttrs[p]=p->seg.attr.value;
    p->seg.attr.bs.reserved=1;
  }
  return p;
}
extern "C" urma_status_t urma_unregister_seg(urma_target_seg_t* s){
  {std::lock_guard<std::mutex> l(mu);auto i=sourceAttrs.find(s);
   if(i!=sourceAttrs.end()){s->seg.attr.value=i->second;sourceAttrs.erase(i);}}
  if(s->seg.attr.bs.access==URMA_ACCESS_READ&&once("unreg"))return URMA_EAGAIN;
  auto start=Clock::now();auto r=next<decltype(&urma_unregister_seg)>("urma_unregister_seg")(s);unregUs+=us(start);event("unreg",0,us(start));if(!r)++unregistered;return r;
}
extern "C" urma_target_seg_t* urma_import_seg(urma_context_t* c,urma_seg_t* s,urma_token_t* t,uint64_t a,urma_import_seg_flag_t f){
  if(once("import")){errno=ENOMEM;return nullptr;}
  auto start=Clock::now();auto p=next<decltype(&urma_import_seg)>("urma_import_seg")(c,s,t,a,f);importUs+=us(start);if(p)++imports;return p;
}
extern "C" urma_status_t urma_unimport_seg(urma_target_seg_t* s){
  if(once("unimport"))return URMA_EAGAIN;
  auto start=Clock::now();auto r=next<decltype(&urma_unimport_seg)>("urma_unimport_seg")(s);unimportUs+=us(start);if(!r)++unimports;return r;
}
extern "C" urma_status_t urma_query_device(urma_device_t* d,urma_device_attr_t* a){
  auto r=next<decltype(&urma_query_device)>("urma_query_device")(d,a);
  if(!r&&std::getenv("TEST_READ_LIMIT"))a->dev_cap.max_read_size=std::stoul(std::getenv("TEST_READ_LIMIT"));
  return r;
}
extern "C" urma_status_t urma_post_jetty_send_wr(urma_jetty_t* j,urma_jfs_wr_t* w,urma_jfs_wr_t** b){
  bool read=w->opcode==URMA_OPC_READ;
  bool offer=w->opcode==URMA_OPC_SEND_IMM&&w->send.imm_data==0x5542523200000001ULL;
  bool ack=w->opcode==URMA_OPC_SEND_IMM&&w->send.imm_data==0x5542523200000002ULL;
  bool data=!read&&!offer&&!ack;
  if((data&&once("send-eagain"))||(read&&once("read-eagain"))||(offer&&once("offer-eagain"))||(ack&&once("ack-eagain"))){*b=w;return URMA_EAGAIN;}
  if(read&&once("read-submit")){*b=w;return URMA_EINVAL;}
  bool forceRnr=(data&&once("send-rnr"))||(offer&&once("offer-rnr"))||(ack&&once("ack-rnr"));
  bool dropAck=ack&&!std::strcmp(fault(),"drop-ack");
  if(forceRnr||dropAck){
    urma_cr_t c{};c.user_ctx=w->user_ctx;c.status=forceRnr?URMA_CR_RNR_RETRY_CNT_EXC_ERR:URMA_CR_SUCCESS;
    std::lock_guard<std::mutex> l(mu);fake[j->jetty_cfg.jfs_cfg.jfc].push_back(c);
    if(dropAck)++injected;return URMA_SUCCESS;
  }
  if(offer&&once("bad-length")){auto p=reinterpret_cast<unsigned char*>(w->send.src.sge[0].addr);std::memset(p+8,0xff,8);}
  if(offer&&once("zero-length")){auto p=reinterpret_cast<unsigned char*>(w->send.src.sge[0].addr);std::memset(p+8,0,8);}
  if(offer&&once("short-region")){auto p=reinterpret_cast<unsigned char*>(w->send.src.sge[0].addr);std::memset(p+24,0,8);}
  if(offer&&once("bad-address")){auto p=reinterpret_cast<unsigned char*>(w->send.src.sge[0].addr);std::memset(p+16,0xff,8);}
  if(offer&&once("bad-attr")){
    auto p=reinterpret_cast<unsigned char*>(w->send.src.sge[0].addr)+36;
    urma_seg_attr_t attr{};
    for(int i=0;i<4;++i)attr.value=(attr.value<<8)|p[i];
    attr.bs.reserved=1;
    for(int i=3;i>=0;--i){p[i]=static_cast<unsigned char>(attr.value);attr.value>>=8;}
  }
  if(ack&&once("bad-ack")){auto p=reinterpret_cast<unsigned char*>(w->send.src.sge[0].addr);p[7]^=1;}
  if(data&&w->send.src.sge[0].len>3968)std::abort();
  auto r=next<decltype(&urma_post_jetty_send_wr)>("urma_post_jetty_send_wr")(j,w,b);
  if(!r&&offer){std::lock_guard<std::mutex> l(mu);lastOffer[j->jetty_cfg.jfs_cfg.jfc]=w->user_ctx;}
  event(read?"post-read":offer?"post-offer":ack?"post-ack":"post-send",w->user_ctx,r);
  if(!r){if(read){++reads;readBytes+=w->rw.src.sge[0].len;}else if(offer)++offers;else if(ack)++acks;else ++sends;}
  return r;
}
extern "C" int urma_poll_jfc(urma_jfc_t* q,int count,urma_cr_t* c){
  int n=next<decltype(&urma_poll_jfc)>("urma_poll_jfc")(q,count,c);
  if(n<0)return n;
  {std::lock_guard<std::mutex> l(mu);
   for(int i=0;i<n;++i){
    const bool paced = !std::strcmp(fault(),"paced-send") && !(c[i].user_ctx&(1ULL<<62));
    bool delay=!c[i].flag.bs.s_r&&c[i].status==URMA_CR_SUCCESS&&
       (paced || ((c[i].user_ctx&(1ULL<<62))&&once("hold-read-cq"))||
        (c[i].user_ctx==lastOffer[q]&&once("hold-offer-cq")));
    if(delay){if(paced)++injected;held[q]={Clock::now()+std::chrono::milliseconds(paced?20:100),c[i]};c[i--]=c[--n];}
   }
   auto h=held.find(q);if(h!=held.end()&&Clock::now()>=h->second.first&&n<count){c[n++]=h->second.second;held.erase(h);}
   auto& pending=fake[q];while(n<count&&!pending.empty()){c[n++]=pending.front();pending.pop_front();}}
  for(int i=0;i<n;++i){
    event(c[i].flag.bs.s_r?"recv-cq":"send-cq",c[i].user_ctx,c[i].status);
    if(!c[i].flag.bs.s_r&&(c[i].user_ctx&(1ULL<<62))&&c[i].status==URMA_CR_SUCCESS&&once("read-cq"))c[i].status=URMA_CR_LOC_ACCESS_ERR;
    if(c[i].status==URMA_CR_RNR_RETRY_CNT_EXC_ERR)++rnr;
  }
  return n;
}
