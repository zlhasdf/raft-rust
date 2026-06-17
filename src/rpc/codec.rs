//! 帧编解码（codec = coder/decoder）。
//!
//! TCP 是「字节流」，本身没有「消息边界」的概念，发送方写两次、接收方可能一次就读到（粘包），
//! 也可能一条消息分几次才读全（拆包）。这里用「长度前缀」约定来切分出一条条完整的帧。

// 这几个是 tokio 提供的异步 IO trait（能力接口）：
// AsyncRead/AsyncWrite 是「可异步读/写」的底层标记；
// AsyncReadExt/AsyncWriteExt 是扩展 trait，给上面两者补充了 read_u32/read_exact/write_all 等便捷方法。
// 只要把扩展 trait `use` 进来，就能在实现了基础 trait 的类型上直接调用这些方法。
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::error::{Result, RpcError};

// 私有常量（没有 pub，只在本文件可见）。
const HEADER_LEN: usize = 1 + 8;                 // 头部固定长度：msg_type(1 字节) + request_id(8 字节)。
const MAX_FRAME_LEN: usize = 32 * 1024 * 1024;   // 单帧上限 32 MiB，防止异常长度撑爆内存。

/// 一条 RPC 帧在内存里的结构化表示。
// `#[derive(...)]` 自动实现：Debug(可打印)、Clone(可复制)、Eq/PartialEq(可用 == 比较，测试里要用到)。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RpcFrame {
    // 字段前的 `pub` 表示该字段对外可见，外部代码可直接读写。
    pub msg_type: u8,    // 消息类型编号（对应 mod.rs 里的 MSG_* 常量）。
    pub request_id: u64, // 请求唯一编号，用来把「响应」和「当初的请求」对上号。
    pub body: Vec<u8>,   // 消息体原始字节（通常是一段 protobuf）。Vec<u8> 是可增长的字节数组。
}

impl RpcFrame {
    /// 便捷构造函数。
    pub fn new(msg_type: u8, request_id: u64, body: Vec<u8>) -> Self {
        // 当「字段名」与「同名局部变量」一致时，可用这种简写，等价于 `msg_type: msg_type` 等。
        Self {
            msg_type,
            request_id,
            body,
        }
    }
}

// 从字节流中读取一个 RPC 帧。
// 这类帧通常由两部分组成：
// 1. 长度前缀：告诉我们后面这帧一共有多少字节；
// 2. 实际内容：按约定依次放 msg_type、request_id 和 body。
//
// `async fn`：异步函数，调用它得到一个 Future，要用 `.await` 才真正推进，期间不阻塞线程。
// `<R>`：泛型参数，让这个函数能接收「任何可读的东西」（TCP 连接、内存管道、文件……）。
// `where R: AsyncRead + Unpin`：泛型约束，要求 R 必须同时满足「可异步读」和 Unpin
//        （Unpin 允许它在内存中被安全移动，是 await 期间持有该值的常见要求）。
// `&mut R`：可变借用——我们要从中读取并推进它的内部读取位置，所以需要 mut。
pub async fn read_frame<R>(reader: &mut R) -> Result<RpcFrame> 
where R: AsyncRead + Unpin {
    // 先读出帧总长度（4 字节大端无符号整数），后续会用它来判断这帧是否合法。
    // `as usize` 是类型转换，把 u32 转成「跟随平台位宽的无符号整数」，方便当长度/索引用。
    let total_len = reader.read_u32().await? as usize;

    // 一帧至少要能放下固定头部；如果连头部都放不下，说明数据格式不对。
    if total_len < HEADER_LEN {
        return Err(RpcError::InvalidFrame(format!(
            "total_len {total_len} is smaller than header"
        )));
    }

    // 再做一次上限保护，避免异常长度导致分配过大内存。
    if total_len > MAX_FRAME_LEN {
        return Err(RpcError::InvalidFrame(format!(
            "total_len {total_len} is larger than max frame len"
        )));
    }

    // 为这一整帧准备缓冲区。`vec![值; 个数]` 生成一个长度为 total_len、初始全为 0 的字节数组。
    // `0_u8` 里的 `_u8` 是类型后缀，明确这是 u8 类型的 0。后面的字段都会从这块字节数组中切出来。
    let mut payload = vec![0_u8; total_len];
    // read_exact：必须把 payload 填满才返回，不够就一直等——保证我们拿到的是「完整一帧」。
    reader.read_exact(&mut payload).await?;
    // 协议约定（`a..b` 是「左闭右开」区间，含 a 不含 b）：
    // payload[0]      -> 消息类型（1 字节）
    // payload[1..9]   -> 请求 ID（8 字节，大端序）
    // payload[9..]    -> 消息体
    let msg_type = payload[0];
    // from_be_bytes：把 8 个字节按大端序(be)拼成一个 u64。
    // try_into 尝试把「长度不定的切片」转成「固定 8 字节数组」；理论上这里长度必然为 8，
    // 所以用 expect 直接断言成功（若失败就带这条信息 panic）。
    let request_id = u64::from_be_bytes(payload[1..9].try_into().expect("slice length"));
    // 从头部之后到末尾的所有字节就是 body；to_vec() 复制成一个独立拥有所有权的 Vec。
    let body = payload[HEADER_LEN..].to_vec();

    // 把解析出的字段重新组装成业务层更容易使用的结构体。
    Ok(RpcFrame {
        msg_type,
        request_id,
        body,
    })
}


// 把一个 RpcFrame 写到字节流里，是 read_frame 的「逆操作」，写入顺序必须和读取顺序严格一致。
// `&RpcFrame` 是不可变借用：只读取 frame 的内容，不获取它的所有权。
// 返回 `Result<()>`：`()` 是「单元类型」（相当于「无返回值」），成功时返回 Ok(())。
pub async fn write_frame<W>(writer: &mut W, frame: &RpcFrame) -> Result<()> 
where W: AsyncWrite + Unpin {
    let total_len = HEADER_LEN + frame.body.len();
    if total_len > MAX_FRAME_LEN {
        return Err(RpcError::InvalidFrame(format!(
            "total_len {total_len} is larger than max frame len"
        )));
    }
    // 严格按协议顺序写：长度前缀 -> msg_type -> request_id(大端) -> body。
    writer.write_u32(total_len as u32).await?;
    writer.write_u8(frame.msg_type).await?;
    writer.write_all(&frame.request_id.to_be_bytes()).await?;
    writer.write_all(&frame.body).await?;
    // flush：把缓冲区里攒着的数据真正推送出去，确保对端能立刻读到，而不是卡在本地缓冲里。
    writer.flush().await?;
    Ok(())
}

// `#[cfg(test)]`：条件编译——这段代码只在执行 `cargo test` 时才编译进来，正常构建会被忽略。
#[cfg(test)]
mod tests {
    // `use super::*;` 把父模块（本文件）里的所有公开项一次性引入，测试里就能直接用 RpcFrame 等。
    use super::*;
    use tokio::io::duplex; // duplex：内存里的一对相连读写端，模拟「客户端<->服务端」管道，免去真实网络。

    // `#[tokio::test]`：标记这是一个异步测试，由 tokio 运行时驱动（普通 #[test] 不能 await）。
    #[tokio::test]
    async fn frame_round_trip() {
        // round_trip（往返）测试：写进去再读出来，应当和原始数据完全相等。
        let (mut client, mut server) = duplex(1024);
        let frame = RpcFrame::new(3, 9, b"hello".to_vec()); // b"hello" 是「字节串字面量」，类型是 &[u8]。
        write_frame(&mut client, &frame).await.unwrap();     // unwrap：测试里图省事，出错就直接 panic 让测试失败。
        assert_eq!(read_frame(&mut server).await.unwrap(), frame); // 断言读回的帧与写入的相等。
    }

    #[tokio::test]
    async fn can_read_multiple_frames() {
        let (mut client, mut server) = duplex(1024);
        let first = RpcFrame::new(1, 1, b"a".to_vec());
        let second = RpcFrame::new(2, 2, b"bb".to_vec());
        write_frame(&mut client, &first).await.unwrap();
        write_frame(&mut client, &second).await.unwrap();
        assert_eq!(read_frame(&mut server).await.unwrap(), first);
        assert_eq!(read_frame(&mut server).await.unwrap(), second);
    }
}