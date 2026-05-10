use std::io::Result;
use tokio::net::TcpStream;

#[inline]
pub async fn run_relay(mut local: TcpStream, mut remote: TcpStream) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::io::ErrorKind;
        use bytehub_io::mem_copy::get_obfs_mode;

        // ── obfs 模式必须强制走用户空间 bidi_copy，禁止走 splice ──────────────
        //
        // 根本原因（双重 Bug）：
        //
        // Bug 1 - first_chunk XOR 解码缺失：
        //   obfs 握手（bidi_copy_buf Mode 1/2）在建立连接时将 XOR(first_chunk)
        //   嵌入握手包尾部写入对端 socket。consume_dynamic_obfs_v2 只读走了
        //   固定的 header+garbage 字节，XOR(first_chunk) 仍留在 socket 缓冲区。
        //   BidiCopy 启动后，第一次读取到的正是这段 XOR 过的字节：
        //     - bidi_copy 路径（用户空间）：poll_read_buf 再 XOR 一次 → 还原原文 ✓
        //     - splice 路径（内核）：完全绕过用户空间，XOR(first_chunk) 原样写给
        //       对端 → 数据损坏，协议报错，连接中断 ✗
        //   只要握手中 first_chunk 非空（用户/上游在 200ms 内有数据），必然触发。
        //
        // Bug 2 - splice 失败降级导致二次握手：
        //   若 BidiCopy 阶段 splice 中途返回 InvalidInput，当前代码降级重调
        //   bidi_copy()，而 bidi_copy() 内部会再次执行 bidi_copy_buf()，
        //   即再次执行 Mode 1/2 握手。但对端已消费过一次握手头，处于数据
        //   传输阶段，两端协议状态不一致 → 永久损坏 ✗
        //
        // 修复：obfs 模式下直接走 bidi_copy（用户空间 + XOR），完全跳过 splice。
        // 非 obfs 模式（mode=0）保持原有 splice 优先路径，不影响性能。
        // ─────────────────────────────────────────────────────────────────────
        if get_obfs_mode() != 0 {
            return bytehub_io::bidi_copy(&mut local, &mut remote).await.map(|_| ());
        }

        match bytehub_io::bidi_zero_copy(&mut local, &mut remote).await {
            Ok(_) => Ok(()),
            Err(ref e) if e.kind() == ErrorKind::InvalidInput => {
                bytehub_io::bidi_copy(&mut local, &mut remote).await.map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        bytehub_io::bidi_copy(&mut local, &mut remote).await.map(|_| ())
    }
}
