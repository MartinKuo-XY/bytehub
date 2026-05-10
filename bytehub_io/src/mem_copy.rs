use std::io::Result;
use std::pin::Pin;
use std::task::{Poll, Context};

use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{CopyBuffer, AsyncIOBuf};
use super::bidi_copy_buf;

// ==============================================================
// Tokio 协程本地存储 (Task Local)
// 0: 原版普通转发 | 1: Client混淆客户端 | 2: Server混淆服务端
// ==============================================================
tokio::task_local! {
    pub static OBFS_MODE: u8;
}

pub fn get_obfs_mode() -> u8 {
    OBFS_MODE.try_with(|x| *x).unwrap_or(0)
}
// ==============================================================

impl<B, SR, SW> AsyncIOBuf for CopyBuffer<B, SR, SW>
where
    B: AsMut<[u8]>,
    SR: AsyncRead + AsyncWrite + Unpin,
    SW: AsyncRead + AsyncWrite + Unpin,
{
    type StreamR = SR;
    type StreamW = SW;

    #[inline]
    fn poll_read_buf(&mut self, cx: &mut Context<'_>, stream: &mut Self::StreamR) -> Poll<Result<usize>> {
        // ===============================================================
        // 【核心修改】：精确到单字节（个位数）的强随机 Chunking (27% ~ 62%)
        // ===============================================================
        let max_cap = self.buf.as_mut().len();
        let mut target_len = max_cap;

        // 仅对较大的缓冲区进行切片，避免极小的数据包被过度碎化
        if max_cap > 128 && get_obfs_mode() != 0 {
            // 1. 获取纳秒作为种子
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos() as usize;
            
            // 2. 计算 27% 和 62% 的精确【绝对字节数】
            let min_bytes = (max_cap * 27) / 100;
            let max_bytes = (max_cap * 62) / 100;
            
            // 3. 在 min_bytes 和 max_bytes 之间取纯随机数
            // 例如：min=2212, max=5079，跨度 range = 2868
            // nanos % 2868 会得到 0~2867 之间精确到个位数的任何一个值
            // 最后加上 min_bytes，得到 2212~5079 之间极其均匀的单字节分布！
            let range = max_bytes - min_bytes + 1;
            target_len = min_bytes + (nanos % range);

            // 4. 安全兜底：如果算出是 0，强行保底 1 字节
            if target_len == 0 {
                target_len = 1;
            }
        }

        // 用被随机截断后的目标长度创建 ReadBuf
        let mut buf = ReadBuf::new(&mut self.buf.as_mut()[..target_len]);
        // ===============================================================
        
        match Pin::new(&mut *stream).poll_read(cx, &mut buf) {
            Poll::Ready(Ok(())) => {
                let len = buf.filled().len();
                
                // 动态检查：只要 mode 不是 0，就对后续全流数据进行 XOR 混淆
                // XOR 是逐字节进行的，完全不受前面切出了多少个单字节的影响！
                if len > 0 {
                    if get_obfs_mode() != 0 {
                        let slice = &mut self.buf.as_mut()[..len];
                        for byte in slice.iter_mut() {
                            *byte ^= 0x5A; // 核心 XOR 密钥
                        }
                    }
                }
                
                Poll::Ready(Ok(len))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    #[inline]
    fn poll_write_buf(&mut self, cx: &mut Context<'_>, stream: &mut Self::StreamW) -> Poll<Result<usize>> {
        Pin::new(&mut *stream).poll_write(cx, &self.buf.as_mut()[self.pos..self.cap])
    }

    #[inline]
    fn poll_flush_buf(&mut self, cx: &mut Context<'_>, stream: &mut Self::StreamW) -> Poll<Result<()>> {
        Pin::new(&mut *stream).poll_flush(cx)
    }
}

pub async fn bidi_copy<A, B>(a: &mut A, b: &mut B) -> Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    let a_to_b_buf = CopyBuffer::new(vec![0u8; buf_size()].into_boxed_slice());
    let b_to_a_buf = CopyBuffer::new(vec![0u8; buf_size()].into_boxed_slice());
    bidi_copy_buf(a, b, a_to_b_buf, b_to_a_buf).await
}

mod buf_ctl {
    pub const DF_BUF_SIZE: usize = 0x2000;
    static mut BUF_SIZE: usize = DF_BUF_SIZE;
    #[inline]
    pub fn buf_size() -> usize { unsafe { BUF_SIZE } }
    #[inline]
    pub fn set_buf_size(n: usize) { unsafe { BUF_SIZE = n } }
}

pub use buf_ctl::{buf_size, set_buf_size};
