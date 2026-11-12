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
#include <unordered_set>
#include <folly/io/async/urma/UrmaProtocol.h>
#include <deque>
namespace {
std::atomic<unsigned long> sends{0},pulls{0},frees{0},reads{0},readBytes{0},registered{0},unregistered{0},imports{0},unimports{0},injected{0},rnr{0};
std::atomic<unsigned long> regUs{0},unregUs{0},importUs{0},unimportUs{0};
std::atomic<unsigned long> readPosts{0}, maxReadChain{0};
std::atomic<unsigned long> revokes{0}, revoked{0};
using Clock=std::chrono::steady_clock;
unsigned long us(Clock::time_point start){return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now()-start).count();}
std::atomic<int> events{0};
void event(const char* name,unsigned long id,unsigned long value){
  if(std::getenv("READ_TIMELINE")&&events++<160)std::fprintf(stderr,"EVENT %lu %s id=%lu value=%lu\n",us(Clock::time_point{}),name,id,value);
}
std::mutex mu;
std::unordered_map<urma_jfc_t*,std::deque<urma_cr_t>> fake;
std::unordered_map<urma_jfc_t*,std::pair<Clock::time_point,urma_cr_t>> held;
std::unordered_map<urma_jfc_t*,uint64_t> lastPull;
std::unordered_map<urma_jfc_t*,std::unordered_set<uint64_t>> readContexts;
std::unordered_map<urma_target_seg_t*,uint32_t> sourceAttrs;
const char* fault(){auto p=std::getenv("READ_FAULT");return p?p:"";}
bool once(const char* mode){return !std::strcmp(fault(),mode)&&!injected.exchange(1);}
template<class T>T next(const char* n){return reinterpret_cast<T>(dlsym(RTLD_NEXT,n));}
__attribute__((destructor))void report(){
  std::fprintf(stderr,"TRACE sends=%lu pulls=%lu frees=%lu reads=%lu read_bytes=%lu reg=%lu unreg=%lu imports=%lu unimports=%lu injected=%lu rnr=%lu\n",sends.load(),pulls.load(),frees.load(),reads.load(),readBytes.load(),registered.load(),unregistered.load(),imports.load(),unimports.load(),injected.load(),rnr.load());
  std::fprintf(stderr,"SDK_TIME reg_us=%lu unreg_us=%lu import_us=%lu unimport_us=%lu\n",regUs.load(),unregUs.load(),importUs.load(),unimportUs.load());
  std::fprintf(stderr,"READ_CHAIN posts=%lu max_wrs=%lu\n",readPosts.load(),maxReadChain.load());
  std::fprintf(stderr,"SEGMENT_REVOKE requests=%lu acknowledgements=%lu\n",revokes.load(),revoked.load());
}
}
extern "C" urma_target_seg_t* urma_register_seg(urma_context_t* c,urma_seg_cfg_t* s){
  if(s->flag.bs.access==URMA_ACCESS_READ&&once("source-reg")){errno=ENOMEM;return nullptr;}
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
extern "C" urma_status_t urma_post_jfs_wr(urma_jfs_t* j,urma_jfs_wr_t* w,urma_jfs_wr_t** b){
  bool read=w->opcode==URMA_OPC_READ;
  auto* packet = read ? nullptr : reinterpret_cast<unsigned char*>(w->send.src.sge[0].addr);
  bool pull=w->opcode==URMA_OPC_SEND_IMM&&packet[6]==uint8_t(folly::urma_detail::PacketKind::Pull);
  bool free=w->opcode==URMA_OPC_SEND_IMM&&packet[6]==uint8_t(folly::urma_detail::PacketKind::Free);
  bool revoke=!read&&packet[6]==uint8_t(folly::urma_detail::PacketKind::SegmentRevoke);
  bool ack=!read&&packet[6]==uint8_t(folly::urma_detail::PacketKind::SegmentRevoked);
  bool data=!read&&!pull&&!free;
  if(free&&!std::strcmp(fault(),"hold-free")) {
    static const auto until=Clock::now()+std::chrono::seconds(1);
    if(Clock::now()<until){++injected;*b=w;return URMA_EAGAIN;}
  }
  if((data&&once("send-eagain"))||(read&&once("read-eagain"))||(pull&&once("pull-eagain"))||(free&&once("free-eagain"))){*b=w;return URMA_EAGAIN;}
  if(read&&once("read-submit")){*b=w;return URMA_EINVAL;}
  bool forceRnr=(data&&once("send-rnr"))||(pull&&once("pull-rnr"))||(free&&once("free-rnr"));
  bool dropFree=free&&!std::strcmp(fault(),"drop-free");
  if(forceRnr||dropFree){
    urma_cr_t c{};c.user_ctx=w->user_ctx;c.status=forceRnr?URMA_CR_RNR_RETRY_CNT_EXC_ERR:URMA_CR_SUCCESS;
    std::lock_guard<std::mutex> l(mu);fake[j->jfs_cfg.jfc].push_back(c);
    if(dropFree)++injected;return URMA_SUCCESS;
  }
  if(pull&&once("bad-length"))std::memset(packet+24,0xff,4);
  if(pull&&once("zero-length"))std::memset(packet+24,0,4);
  if(pull&&once("bad-address"))w->send.imm_data=UINT64_MAX;
  if(pull&&once("bad-reserved"))packet[31]=1;
  if(free&&once("bad-free"))packet[23]^=1;
  if(data&&w->send.src.sge[0].len>3992)std::abort();
  auto r=next<decltype(&urma_post_jfs_wr)>("urma_post_jfs_wr")(j,w,b);
  if(read){
    ++readPosts;
    unsigned long count=0;
    std::lock_guard<std::mutex> l(mu);
    for(auto* item=w;item;item=item->next){
      if(item->opcode!=URMA_OPC_READ||!item->flag.bs.complete_enable)std::abort();
      ++count;
    }
    if(count>maxReadChain.load())maxReadChain=count;
    // On partial acceptance, only the prefix can produce real CQEs.
    for(auto* item=w;item&&(!r||(*b&&item!=*b));item=item->next){
      readContexts[j->jfs_cfg.jfc].insert(item->user_ctx);
      ++reads;
      for(unsigned i=0;i<item->rw.src.num_sge;++i)readBytes+=item->rw.src.sge[i].len;
      event("post-read",item->user_ctx,r);
    }
    return r;
  }
  if(!r){std::lock_guard<std::mutex> l(mu);
    if(pull)lastPull[j->jfs_cfg.jfc]=w->user_ctx;
    if(read)readContexts[j->jfs_cfg.jfc].insert(w->user_ctx);
  }
  event(read?"post-read":pull?"post-pull":free?"post-free":"post-send",w->user_ctx,r);
  if(!r){if(read){++reads;readBytes+=w->rw.src.sge[0].len;}else if(pull)++pulls;else if(free)++frees;else ++sends;
    if(revoke)++revokes;if(ack)++revoked;
  }
  return r;
}
extern "C" int urma_poll_jfc(urma_jfc_t* q,int count,urma_cr_t* c){
  int n=next<decltype(&urma_poll_jfc)>("urma_poll_jfc")(q,count,c);
  if(n<0)return n;
  {std::lock_guard<std::mutex> l(mu);
   for(int i=0;i<n;++i){
    const bool paced = !std::strcmp(fault(),"paced-send") && !readContexts[q].count(c[i].user_ctx);
    bool delay=!c[i].flag.bs.s_r&&c[i].status==URMA_CR_SUCCESS&&
       (paced || (readContexts[q].count(c[i].user_ctx)&&once("hold-read-cq"))||
        (c[i].user_ctx==lastPull[q]&&once("hold-pull-cq")));
    if(delay){if(paced)++injected;held[q]={Clock::now()+std::chrono::milliseconds(paced?20:100),c[i]};c[i--]=c[--n];}
   }
   auto h=held.find(q);if(h!=held.end()&&Clock::now()>=h->second.first&&n<count){c[n++]=h->second.second;held.erase(h);}
   auto& pending=fake[q];while(n<count&&!pending.empty()){c[n++]=pending.front();pending.pop_front();}}
  for(int i=0;i<n;++i){
    event(c[i].flag.bs.s_r?"recv-cq":"send-cq",c[i].user_ctx,c[i].status);
    if(!c[i].flag.bs.s_r){std::lock_guard<std::mutex> l(mu);
      if(readContexts[q].erase(c[i].user_ctx)&&c[i].status==URMA_CR_SUCCESS&&once("read-cq"))c[i].status=URMA_CR_LOC_ACCESS_ERR;
    }
    if(c[i].status==URMA_CR_RNR_RETRY_CNT_EXC_ERR)++rnr;
  }
  return n;
}
