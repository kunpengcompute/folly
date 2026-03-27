# API参考

## 函数说明

folly io_uring 优化补丁仓已优化和新增的函数如[**表 1** folly io_uring优化函数列表](#folly_io_uring优化函数列表)所示。

**表 1** folly io_uring优化函数列表<a id="folly_io_uring优化函数列表"></a>

|名称|说明|
|--|--|
|newSocket|新增接口，支持与AsyncSocket相同的同步连接功能。|
|writeChain|修改写逻辑，将WriteSqe替换为连续写，按顺序存入写队列，通过send执行发送。|
|PollWriteSqe|新增接口，通过io_uring接收可写通知，无足够buffer时poll对应的socket fd。|

## 函数定义

### writeChain

**函数功能**

在 AsyncIoUringSocket 中处理写操作。优化版本中移除了 `writeSqeActive` 跟踪，不再通过 io_uring WriteSqe 发送写，而是将写请求按顺序存入写队列，并连续调用 send 按队列顺序执行发送。此混合模式回退到使用原生 send 系统调用，规避了 io_uring 在保序场景下可能引入的延迟。

**函数定义**

```cpp
void AsyncIoUringSocket::writeChain(
        WriteCallback* callback, std::unique_ptr<IOBuf>&& buf, WriteFlags flags);
```

**参数说明**

|参数名|描述|取值范围|输入/输出|
|--|--|--|--|
|callback|写Callback函数|有效的WriteCallback对象指针|输入|
|buf|待写数据存放的IObuf|包含数据的IOBuf智能指针|输入|
|flags|写操作的flags|有效的WriteFlags标志位|输入|

**返回值**

返回值为空。

>![](public_sys-resources/icon-note.gif) **说明：** 
>该函数属于内部接口。当 send 无足够 buffer 时，会结合新增的 `PollWriteSqe` 接口，通过 io_uring poll 对应的 socket fd，在 socket 可写后继续执行发送。
