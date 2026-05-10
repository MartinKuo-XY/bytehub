use std::io::Result;
use tokio::net::TcpStream;

#[inline]
pub async fn run_relay(mut local: TcpStream, mut remote: TcpStream) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::io::ErrorKind;
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
