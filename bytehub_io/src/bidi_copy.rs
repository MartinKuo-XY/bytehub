use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::future::Future;

use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};

use super::{AsyncIOBuf, CopyBuffer};
use super::mem_copy::get_obfs_mode;

enum TransferState<B, SR, SW> {
    Running(CopyBuffer<B, SR, SW>),
    ShuttingDown(u64),
    Done(u64),
}

fn transfer<B, SL, SR>(
    cx: &mut Context<'_>,
    state: &mut TransferState<B, SL, SR>,
    r: &mut SL,
    w: &mut SR,
) -> Poll<Result<u64>>
where
    B: Unpin,
    SL: AsyncRead + AsyncWrite + Unpin,
    SR: AsyncRead + AsyncWrite + Unpin,
    CopyBuffer<B, SL, SR>: AsyncIOBuf<StreamR = SL, StreamW = SR>,
    CopyBuffer<B, SR, SL>: AsyncIOBuf<StreamR = SR, StreamW = SL>,
{
    loop {
        match state {
            TransferState::Running(buf) => {
                let count = ready!(buf.poll_copy(cx, r, w))?;
                *state = TransferState::ShuttingDown(count);
            }
            TransferState::ShuttingDown(count) => {
                ready!(Pin::new(&mut *w).poll_shutdown(cx))?;
                *state = TransferState::Done(*count);
            }
            TransferState::Done(count) => return Poll::Ready(Ok(*count)),
        }
    }
}

fn transfer2<B, SL, SR>(
    cx: &mut Context<'_>,
    state: &mut TransferState<B, SR, SL>,
    r: &mut SR,
    w: &mut SL,
) -> Poll<Result<u64>>
where
    B: Unpin,
    SL: AsyncRead + AsyncWrite + Unpin,
    SR: AsyncRead + AsyncWrite + Unpin,
    CopyBuffer<B, SL, SR>: AsyncIOBuf<StreamR = SL, StreamW = SR>,
    CopyBuffer<B, SR, SL>: AsyncIOBuf<StreamR = SR, StreamW = SL>,
{
    loop {
        match state {
            TransferState::Running(buf) => {
                let count = ready!(buf.poll_copy(cx, r, w))?;
                *state = TransferState::ShuttingDown(count);
            }
            TransferState::ShuttingDown(count) => {
                ready!(Pin::new(&mut *w).poll_shutdown(cx))?;
                *state = TransferState::Done(*count);
            }
            TransferState::Done(count) => return Poll::Ready(Ok(*count)),
        }
    }
}

struct BidiCopy<'a, B, SL, SR>
where
    B: Unpin,
    SL: AsyncRead + AsyncWrite + Unpin,
    SR: AsyncRead + AsyncWrite + Unpin,
    CopyBuffer<B, SL, SR>: AsyncIOBuf<StreamR = SL, StreamW = SR>,
    CopyBuffer<B, SR, SL>: AsyncIOBuf<StreamR = SR, StreamW = SL>,
{
    a: &'a mut SL,
    b: &'a mut SR,
    a_to_b: TransferState<B, SL, SR>,
    b_to_a: TransferState<B, SR, SL>,
}

impl<'a, B, SL, SR> Future for BidiCopy<'a, B, SL, SR>
where
    B: Unpin,
    SL: AsyncRead + AsyncWrite + Unpin,
    SR: AsyncRead + AsyncWrite + Unpin,
    CopyBuffer<B, SL, SR>: AsyncIOBuf<StreamR = SL, StreamW = SR>,
    CopyBuffer<B, SR, SL>: AsyncIOBuf<StreamR = SR, StreamW = SL>,
{
    type Output = Result<(u64, u64)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let BidiCopy { a, b, a_to_b, b_to_a } = self.get_mut();

        let a_to_b = transfer(cx, a_to_b, a, b)?;
        let b_to_a = transfer2::<B, SL, SR>(cx, b_to_a, b, a)?;

        #[cfg(not(feature = "brutal-shutdown"))]
        {
            let a_to_b = ready!(a_to_b);
            let b_to_a = ready!(b_to_a);
            Poll::Ready(Ok((a_to_b, b_to_a)))
        }

        #[cfg(feature = "brutal-shutdown")]
        {
            match (a_to_b, b_to_a) {
                (Poll::Ready(a), Poll::Ready(b)) => Poll::Ready(Ok((a, b))),
                (Poll::Pending, Poll::Ready(b)) => Poll::Ready(Ok((0, b))),
                (Poll::Ready(a), Poll::Pending) => Poll::Ready(Ok((a, 0))),
                _ => Poll::Pending,
            }
        }
    }
}

// ============================================================
// 简易 LCG 伪随机数生成器（无外部依赖）
// ============================================================
struct SimpleRng { state: u64 }
impl SimpleRng {
    fn new() -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self { state: seed }
    }
    fn next_range(&mut self, min: u32, max: u32) -> u32 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let val = (self.state >> 32) as u32;
        min + (val % (max - min + 1))
    }
}

// ============================================================
// V3 混淆头格式（14 字节 = 10 字节内容区 + 4 字节认证标签）
//
// ── 内容区 bytes[0..10]（全随机，qualifying >= 3）──
//   垃圾长度编码：过滤 a-z/0-9 → 按出现顺序排列 → 取最后 3 个
//   每字符 → 单数字：数字字符直接用；字母取位置(1-26)的个位(a→1, j→0, z→6)
//   3 位数字拼接 → parse 为 usize = 垃圾字节数（范围 000-999）
//
// ── 认证标签 bytes[10..14]（4 字节键控哈希）──
//   tag = keyed_hash_tag(raw_content[0..10], obfs_key)
//   基于键控 FNV-1a + avalanche 混合，无需外部 crate
//
// 发送流程：raw_header(14B) → XOR obfs_key → wire，后接随机垃圾
// 接收流程：read(14B) → XOR decode → 验证 tag → 验证 qualifying → 丢弃垃圾
//
// ── 探测检测概率 ──
//   主验证（tag 4 字节）：随机命中概率 = 1/2³² ≈ 2.3×10⁻¹⁰
//   副验证（qualifying >= 3）：随机命中概率 ≈ 18.2%
//   综合：探测者通过概率 ≈ 4.2×10⁻¹¹  →  检测率 ≈ 99.99999996%
//
// 例：内容区过滤后最后 3 为 q,1,2
//     q(17→个位7) + 1 + 2  →  "712"  →  712 字节垃圾
// 例：最后 3 为 z,z,y
//     z(26→6) + z(26→6) + y(25→5)  →  "665"  →  665 字节垃圾
// ============================================================

/// 键控哈希，输出 4 字节认证标签。
/// 算法：键控 FNV-1a（先混入密钥再混入数据）+ 最终 avalanche 混合。
/// 纯 Rust 实现，零外部依赖。
#[inline]
fn keyed_hash_tag(data: &[u8], key: &[u8]) -> [u8; 4] {
    const FNV_OFFSET: u32 = 0x811c_9dc5;
    const FNV_PRIME:  u32 = 0x0100_0193;

    let mut h: u32 = FNV_OFFSET;

    // ① 混入密钥（使哈希结果与密钥绑定，不知道密钥就无法伪造）
    for &k in key {
        h ^= k as u32;
        h = h.wrapping_mul(FNV_PRIME);
    }
    // ② 混入内容数据
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(FNV_PRIME);
    }
    // ③ Avalanche 最终混合（消除线性偏差，增强低位随机性）
    h ^= h >> 16;
    h = h.wrapping_mul(0x45d9_f3b);
    h ^= h >> 16;

    h.to_le_bytes()
}

/// 从原始（已 XOR 解码）的 10 字节内容区中计算垃圾数据长度。
/// 规则：过滤 a-z / 0-9 → 取后 3 个 → 每字符转单数字 → 拼接为 3 位整数。
fn compute_garbage_len(raw: &[u8]) -> usize {
    // 过滤 a-z 和 0-9，按出现顺序保留
    let filtered: Vec<u8> = raw
        .iter()
        .copied()
        .filter(|&b| (b'a'..=b'z').contains(&b) || (b'0'..=b'9').contains(&b))
        .collect();

    // 取最后 3 个（调用方已确保 filtered.len() >= 3）
    let last3 = &filtered[filtered.len() - 3..];

    // 每个字符转换为 0-9 单个数字
    let mut len_str = String::with_capacity(3);
    for &b in last3 {
        let digit = if (b'0'..=b'9').contains(&b) {
            b - b'0'
        } else {
            // 字母 a-z：位置 1-26，取个位（% 10）
            // a=1→1, j=10→0, t=20→0, z=26→6
            (b - b'a' + 1) % 10
        };
        len_str.push((b'0' + digit) as char);
    }

    len_str.parse::<usize>().unwrap_or(0)
}

/// 生成 V3 混淆前缀（14 字节 header + 垃圾数据），不含真实通信数据。
fn generate_dynamic_obfs_v2(key: &str) -> Vec<u8> {
    let mut rng = SimpleRng::new();
    let key_bytes = key.as_bytes();

    // ── ① 生成全随机 10 字节内容区，循环直至 qualifying >= 3 ────────────
    let mut raw_content = [0u8; 10];
    loop {
        for b in raw_content.iter_mut() {
            *b = rng.next_range(0, 255) as u8;
        }
        let qualifying = raw_content
            .iter()
            .filter(|&&b| (b'a'..=b'z').contains(&b) || (b'0'..=b'9').contains(&b))
            .count();
        if qualifying >= 3 {
            break;
        }
    }

    // ── ② 计算垃圾长度（基于原始内容区）────────────────────────────────
    let garbage_len = compute_garbage_len(&raw_content);

    // ── ③ 计算 4 字节认证标签（基于原始内容区 + 密钥）──────────────────
    let tag = keyed_hash_tag(&raw_content, key_bytes);

    // ── ④ 组装 14 字节原始 header，整体 XOR 编码 ────────────────────────
    let mut raw_header = [0u8; 14];
    raw_header[..10].copy_from_slice(&raw_content);
    raw_header[10..].copy_from_slice(&tag);

    for (i, byte) in raw_header.iter_mut().enumerate() {
        *byte ^= key_bytes[i % key_bytes.len()];
    }

    // ── ⑤ 生成随机垃圾数据 ──────────────────────────────────────────────
    let mut garbage = vec![0u8; garbage_len];
    for b in garbage.iter_mut() {
        *b = rng.next_range(0, 255) as u8;
    }

    let mut out = Vec::with_capacity(14 + garbage_len);
    out.extend_from_slice(&raw_header);
    out.extend(garbage);
    out
}

/// 从流中消费并验证 V3 混淆头，然后丢弃垃圾数据。
/// 认证失败或格式异常（疑似主动探测）：等待 3 秒后返回错误。
async fn consume_dynamic_obfs_v2<S>(stream: &mut S, key: &str) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let key_bytes = key.as_bytes();

    // ── ① 读取 14 字节 header 并 XOR 解码 ───────────────────────────────
    let mut header = [0u8; 14];
    stream.read_exact(&mut header).await?;
    for (i, byte) in header.iter_mut().enumerate() {
        *byte ^= key_bytes[i % key_bytes.len()];
    }

    // ── ② 主验证：认证标签（4 字节键控哈希）────────────────────────────
    //    不知道密钥的探测者通过概率 = 1/2³² ≈ 2.3×10⁻¹⁰
    let expected_tag = keyed_hash_tag(&header[..10], key_bytes);
    if header[10..14] != expected_tag {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Active probe silently dropped",
        ));
    }

    // ── ③ 副验证：内容区 qualifying >= 3（合法发送方必然满足）───────────
    let qualifying = header[..10]
        .iter()
        .filter(|&&b| (b'a'..=b'z').contains(&b) || (b'0'..=b'9').contains(&b))
        .count();
    if qualifying < 3 {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Active probe silently dropped",
        ));
    }

    // ── ④ 计算垃圾长度并丢弃 ────────────────────────────────────────────
    let garbage_len = compute_garbage_len(&header[..10]);
    let mut garbage = vec![0u8; garbage_len];
    stream.read_exact(&mut garbage).await?;
    Ok(())
}

/// 从流中读取第一个数据块（带 200 ms 超时）。
/// 超时则返回空 Vec（意味着没有立即可读的数据，混淆头将单独发出）。
async fn read_first_chunk<S>(stream: &mut S) -> Vec<u8>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut buf = vec![0u8; 8192];
    match tokio::time::timeout(
        std::time::Duration::from_millis(200),
        stream.read(&mut buf),
    )
    .await
    {
        Ok(Ok(n)) if n > 0 => {
            buf.truncate(n);
            buf
        }
        _ => Vec::new(),
    }
}

pub async fn bidi_copy_buf<B, SR, SW>(
    a: &mut SR,
    b: &mut SW,
    a_to_b_buf: CopyBuffer<B, SR, SW>,
    b_to_a_buf: CopyBuffer<B, SW, SR>,
) -> Result<(u64, u64)>
where
    B: Unpin,
    SR: AsyncRead + AsyncWrite + Unpin,
    SW: AsyncRead + AsyncWrite + Unpin,
    CopyBuffer<B, SR, SW>: AsyncIOBuf<StreamR = SR, StreamW = SW>,
    CopyBuffer<B, SW, SR>: AsyncIOBuf<StreamR = SW, StreamW = SR>,
{
    let obfs_key = "bytehub_obfs_key_2024";

    match get_obfs_mode() {
        // ──────────────────────────────────────────────────────────────
        // Mode 1：客户端
        //   1. 从 a（本地客户端）读取第一块真实数据
        //   2. 将混淆头 + 垃圾 + 真实数据合并为一个 write_all，发向 b（服务端）
        //   3. 解析并丢弃服务端回送的混淆头
        //   4. 启动双向 BidiCopy
        // ──────────────────────────────────────────────────────────────
        1 => {
            // ── 步骤 1：读取第一块真实数据（含超时） ──
            let first_chunk = read_first_chunk(a).await;

            // ── 步骤 2：构造合并包 [obfs_prefix | XOR(first_chunk)] 并一次写出 ──
            // first_chunk 是在 BidiCopy 启动前已从 a 读出的原始明文字节。
            // BidiCopy 的 poll_read_buf 会对后续从 a 读取的数据逐字节 XOR 0x5A，
            // 但此处直接 write_all 到 b，绕过了 poll_read_buf 的 XOR 路径，
            // 所以 first_chunk 必须在此处手动 XOR 0x5A，确保 b（服务端）收到的
            // first_chunk 格式与 BidiCopy 阶段一致（均为 XOR 0x5A 后的密文）。
            let obfs_prefix = generate_dynamic_obfs_v2(obfs_key);
            let mut combined = obfs_prefix;
            if !first_chunk.is_empty() {
                let xored: Vec<u8> = first_chunk.iter().map(|&b| b ^ 0x5A).collect();
                combined.extend_from_slice(&xored);
            }
            b.write_all(&combined).await?;
            // ── 关键：不 flush，依赖内核 Nagle 合并 ──

            // ── 步骤 3：剥离服务端发来的混淆头 ──
            consume_dynamic_obfs_v2(b, obfs_key).await?;

            // ── 步骤 4：启动双向 BidiCopy ──
            let a_to_b = TransferState::Running(a_to_b_buf);
            let b_to_a = TransferState::Running(b_to_a_buf);
            BidiCopy { a, b, a_to_b, b_to_a }.await
        }

        // ──────────────────────────────────────────────────────────────
        // Mode 2：服务端
        //   1. 解析并丢弃客户端发来的混淆头
        //   2. 从 b（上游服务）读取第一块真实数据（带超时）
        //   3. 将混淆头 + 垃圾 + 真实数据合并为一个 write_all，回送给 a（客户端）
        //   4. 启动双向 BidiCopy
        // ──────────────────────────────────────────────────────────────
        2 => {
            // ── 步骤 1：剥离客户端混淆头 ──
            consume_dynamic_obfs_v2(a, obfs_key).await?;

            // ── 步骤 2：读取上游第一块真实数据（含超时） ──
            let first_chunk = read_first_chunk(b).await;

            // ── 步骤 3：构造合并包 [obfs_prefix | XOR(first_chunk)] 并一次写回客户端 ──
            // first_chunk 来自上游 b，是原始明文。
            // 客户端（a 端）的 poll_read_buf 会对 BidiCopy 阶段读到的数据 XOR 0x5A，
            // 但此处直接 write_all 到 a，绕过了 poll_read_buf，
            // 故需手动 XOR 0x5A，使客户端收到的数据格式与 BidiCopy 后续流一致。
            let obfs_prefix = generate_dynamic_obfs_v2(obfs_key);
            let mut combined = obfs_prefix;
            if !first_chunk.is_empty() {
                let xored: Vec<u8> = first_chunk.iter().map(|&b| b ^ 0x5A).collect();
                combined.extend_from_slice(&xored);
            }
            a.write_all(&combined).await?;
            // ── 关键：不 flush ──

            // ── 步骤 4：启动双向 BidiCopy ──
            let a_to_b = TransferState::Running(a_to_b_buf);
            let b_to_a = TransferState::Running(b_to_a_buf);
            BidiCopy { a, b, a_to_b, b_to_a }.await
        }

        // ──────────────────────────────────────────────────────────────
        // Mode 0（或其他）：普通转发，无混淆
        // ──────────────────────────────────────────────────────────────
        _ => {
            let a_to_b = TransferState::Running(a_to_b_buf);
            let b_to_a = TransferState::Running(b_to_a_buf);
            BidiCopy { a, b, a_to_b, b_to_a }.await
        }
    }
}
