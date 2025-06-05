use super::common::*;
use bincode::{Decode, Encode};
use moonlink_backend::{MoonlinkBackend, ReadState};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};

static BACKEND: LazyLock<MoonlinkBackend<TableId>> = LazyLock::new(MoonlinkBackend::default);

#[tokio::main]
pub(super) async fn start() {
    let mut sigterm = signal(SignalKind::terminate())
        .unwrap_or_else(|e| panic!("error setting SIGTERM handler: {e}"));
    LazyLock::force(&BACKEND);
    if fs::metadata(SOCKET_PATH).await.is_ok() {
        fs::remove_file(SOCKET_PATH)
            .await
            .unwrap_or_else(|_| panic!("error removing socket: {SOCKET_PATH}"));
    }
    let listener = UnixListener::bind(SOCKET_PATH)
        .unwrap_or_else(|_| panic!("error binding socket: {SOCKET_PATH}"));
    loop {
        tokio::select! {
            _ = sigterm.recv() => break,
            Ok((stream, _addr)) = listener.accept() => {
                tokio::spawn(async move { handle_stream(stream).await });
            }
        }
    }
}

async fn handle_stream(mut stream: UnixStream) -> Result<(), Eof> {
    let mut map = HashMap::new();
    loop {
        match read(&mut stream).await? {
            Request::CreateSnapshot { table_id, lsn } => {
                create_snapshot(&mut stream, table_id, lsn).await?
            }
            Request::CreateTable {
                table_id,
                uri,
                table,
            } => create_table(&mut stream, table_id, table, uri).await?,
            Request::ScanTableBegin { table_id, lsn } => {
                scan_table_begin(&mut stream, &mut map, table_id, lsn).await?
            }
            Request::ScanTableEnd { table_id } => {
                scan_table_end(&mut stream, &mut map, table_id).await?
            }
        }
    }
}

async fn create_snapshot(stream: &mut UnixStream, table_id: TableId, lsn: u64) -> Result<(), Eof> {
    check_moonlink(BACKEND.create_iceberg_snapshot(&table_id, lsn).await);
    write(stream, &()).await
}

async fn create_table(
    stream: &mut UnixStream,
    table_id: TableId,
    table: String,
    uri: String,
) -> Result<(), Eof> {
    check_moonlink(BACKEND.create_table(table_id, &table, &uri).await);
    write(stream, &()).await
}

async fn scan_table_begin(
    stream: &mut UnixStream,
    map: &mut HashMap<TableId, Arc<ReadState>>,
    table_id: TableId,
    lsn: u64,
) -> Result<(), Eof> {
    let state = check_moonlink(BACKEND.scan_table(&table_id, Some(lsn)).await);
    write(stream, &state.data).await?;
    map.insert(table_id, state);
    Ok(())
}

async fn scan_table_end(
    stream: &mut UnixStream,
    map: &mut HashMap<TableId, Arc<ReadState>>,
    table_id: TableId,
) -> Result<(), Eof> {
    map.remove(&table_id);
    write(stream, &()).await
}

async fn write<E: Encode>(stream: &mut UnixStream, data: &E) -> Result<(), Eof> {
    let bytes = bincode::encode_to_vec(data, BINCODE_CONFIG)
        .unwrap_or_else(|e| panic!("error encoding packet: {e}"));
    let len = u32::try_from(bytes.len()).unwrap_or_else(|_| panic!("packet too long"));
    check_eof(stream.write_all(&len.to_ne_bytes()).await)?;
    check_eof(stream.write_all(&bytes).await)
}

async fn read<D: Decode<()>>(stream: &mut UnixStream) -> Result<D, Eof> {
    let mut buf = [0; 4];
    check_eof(stream.read_exact(&mut buf).await)?;
    let len = u32::from_ne_bytes(buf);
    let mut bytes = vec![0; len as usize];
    check_eof(stream.read_exact(&mut bytes).await)?;
    let (data, _) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)
        .unwrap_or_else(|e| panic!("error decoding packet: {e}"));
    Ok(data)
}

struct Eof;

fn check_eof<T>(r: std::io::Result<T>) -> Result<T, Eof> {
    use std::io::ErrorKind::*;
    match r {
        Ok(t) => Ok(t),
        Err(e) => match e.kind() {
            BrokenPipe | ConnectionReset | UnexpectedEof => Err(Eof),
            _ => panic!("IO error: {e}"),
        },
    }
}

fn check_moonlink<T>(r: moonlink_backend::Result<T>) -> T {
    r.unwrap_or_else(|e| panic!("moonlink error: {e}"))
}
