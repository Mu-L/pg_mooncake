use super::common::*;
use bincode::{Decode, Encode};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::{LazyLock, Mutex};

static STREAM: LazyLock<Mutex<UnixStream>> = LazyLock::new(|| {
    Mutex::new(
        UnixStream::connect(SOCKET_PATH)
            .unwrap_or_else(|e| panic!("error connecting to socket: {e}")),
    )
});

pub(crate) fn create_snapshot(database_id: u32, table_id: u32, lsn: u64) {
    let mut stream = STREAM.lock().unwrap();
    let table_id = TableId {
        database_id,
        table_id,
    };
    write(&mut stream, &Request::CreateSnapshot { table_id, lsn });
    read(&mut stream)
}

pub(crate) fn create_table(database_id: u32, table_id: u32, table: String, uri: String) {
    let mut stream = STREAM.lock().unwrap();
    let table_id = TableId {
        database_id,
        table_id,
    };
    write(
        &mut stream,
        &Request::CreateTable {
            table_id,
            table,
            uri,
        },
    );
    read(&mut stream)
}

pub(super) fn scan_table_begin(table_id: TableId, lsn: u64) -> Vec<u8> {
    let mut stream = STREAM.lock().unwrap();
    write(&mut stream, &Request::ScanTableBegin { table_id, lsn });
    read(&mut stream)
}

pub(super) fn scan_table_end(table_id: TableId) {
    let mut stream = STREAM.lock().unwrap();
    write(&mut stream, &Request::ScanTableEnd { table_id });
    read(&mut stream)
}

fn write<E: Encode>(stream: &mut UnixStream, data: &E) {
    let bytes = bincode::encode_to_vec(data, BINCODE_CONFIG)
        .unwrap_or_else(|e| panic!("error encoding packet: {e}"));
    let len = u32::try_from(bytes.len()).unwrap_or_else(|_| panic!("packet too long"));
    check_io(stream.write_all(&len.to_ne_bytes()));
    check_io(stream.write_all(&bytes));
}

fn read<D: Decode<()>>(stream: &mut UnixStream) -> D {
    let mut buf = [0; 4];
    check_io(stream.read_exact(&mut buf));
    let len = u32::from_ne_bytes(buf);
    let mut bytes = vec![0; len as usize];
    check_io(stream.read_exact(&mut bytes));
    let (data, _) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)
        .unwrap_or_else(|e| panic!("error decoding packet: {e}"));
    data
}

fn check_io<T>(r: std::io::Result<T>) -> T {
    r.unwrap_or_else(|e| panic!("IO error: {e}"))
}
