#!/usr/bin/env python3
"""Nó P2P simétrico: servidor TCP assíncrono e cliente de blocos."""

import argparse
import asyncio
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import struct
import sys
import time
import threading
from collections import Counter
from datetime import datetime, timezone

MAX_HEADER = 8 * 1024 * 1024
MAX_PAYLOAD = 8 * 1024 * 1024
NETWORK_TIMEOUT = 15.0
BINARY_FLAG = getattr(os, "O_BINARY", 0)  # Evita tradução de bytes no Windows.


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def create_manifest(path, block_size):
    hashes = []
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        while block := file.read(block_size):
            hashes.append(hashlib.sha256(block).hexdigest())
            digest.update(block)
    return {
        "protocol": "p2p-tp2-v1",
        "filename": Path(path).name,
        "file_size": os.path.getsize(path),
        "block_size": block_size,
        "block_count": len(hashes),
        "file_sha256": digest.hexdigest(),
        "block_sha256": hashes,
    }


def validate_manifest(meta):
    needed = ("protocol", "filename", "file_size", "block_size", "block_count",
              "file_sha256", "block_sha256")
    if not isinstance(meta, dict) or not all(k in meta for k in needed):
        raise ValueError("Metadados incompletos")
    if meta["protocol"] != "p2p-tp2-v1":
        raise ValueError("Versão de protocolo incompatível")
    if (not isinstance(meta["filename"], str) or
            Path(meta["filename"]).name != meta["filename"] or
            meta["filename"] in ("", ".", "..")):
        raise ValueError("Nome de arquivo inválido")
    size, block, count = meta["file_size"], meta["block_size"], meta["block_count"]
    if any(type(i) is not int for i in (size, block, count)):
        raise ValueError("Tamanhos inválidos")
    if size < 1 or not 1 <= block <= MAX_PAYLOAD:
        raise ValueError("Parâmetros de fragmentação inválidos")
    if count != math.ceil(size / block) or count > 100000:
        raise ValueError("Quantidade de blocos incoerente")
    hashes = meta["block_sha256"]
    if not isinstance(hashes, list) or len(hashes) != count:
        raise ValueError("Lista de hashes incoerente")
    for digest in [meta["file_sha256"], *hashes]:
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Checksum SHA-256 inválido")


async def send_frame(writer, header, payload=b""):
    message = dict(header)
    message["payload_size"] = len(payload)
    encoded = json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > MAX_HEADER or len(payload) > MAX_PAYLOAD:
        raise ValueError("Mensagem excedeu o limite")
    writer.write(struct.pack("!I", len(encoded)) + encoded + payload)
    await writer.drain()


async def receive_frame(reader):
    length = struct.unpack("!I", await reader.readexactly(4))[0]
    if not 0 < length <= MAX_HEADER:
        raise ValueError("Cabeçalho inválido")
    header = json.loads((await reader.readexactly(length)).decode("utf-8"))
    payload_size = header.get("payload_size", 0)
    if type(payload_size) is not int or not 0 <= payload_size <= MAX_PAYLOAD:
        raise ValueError("Tamanho de payload inválido")
    return header, await reader.readexactly(payload_size)


def decode_neighbor(value):
    host, sep, port_text = value.rpartition(":")
    if not sep or not host or not port_text.isdecimal() or not 1 <= int(port_text) <= 65535:
        raise argparse.ArgumentTypeError("Use IP:PORTA, por exemplo 127.0.0.1:9101")
    return host, int(port_text)


class Peer:
    def __init__(self, args):
        self.args = args
        self.name = args.name
        self.dir = Path(args.data_dir).resolve()
        self.dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.dir / "events.jsonl"
        self.log_file = open(self.log_path, "w", encoding="utf-8", buffering=1)
        self.meta = None
        self.have = bytearray()
        self.reserved = bytearray()
        self.lock = asyncio.Lock()
        self.disk_lock = threading.Lock()  # Compatibilidade Windows sem os.pread/pwrite
        self.fd = None
        self.is_seed = bool(args.seed_file)
        self.finished = asyncio.Event()
        self.sources = Counter()
        self.blocks_received = 0
        self.download_start = None
        self.server = None
        self.temp_path = None

    def log(self, event, **details):
        record = {"timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                  "peer": self.name, "event": event, **details}
        self.log_file.write(json.dumps(record, ensure_ascii=False) + "\n")
        if event in ("READY", "METADATA_READY", "TRANSFER_COMPLETE", "ERROR"):
            print(json.dumps(record, ensure_ascii=False), flush=True)

    def set_metadata(self, manifest):
        validate_manifest(manifest)
        self.meta = manifest
        size = (manifest["block_count"] + 7) // 8
        self.have = bytearray(size)
        self.reserved = bytearray(size)
        (self.dir / "metadata.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    def load_seed(self):
        path = Path(self.args.seed_file).resolve()
        manifest = create_manifest(path, self.args.block_size)
        self.set_metadata(manifest)
        self.fd = os.open(path, os.O_RDONLY | BINARY_FLAG)
        for index in range(manifest["block_count"]):
            self.have[index // 8] |= 1 << (index % 8)
        self.log("METADATA_READY", role="seed", blocks=manifest["block_count"],
                 file_size=manifest["file_size"], block_size=manifest["block_size"],
                 sha256=manifest["file_sha256"])

    def read_at(self, length, offset):
        if hasattr(os, "pread"):
            return os.pread(self.fd, length, offset)
        # Em Windows, a operação de posicionamento + leitura precisa ser atômica.
        with self.disk_lock:
            os.lseek(self.fd, offset, os.SEEK_SET)
            return os.read(self.fd, length)

    def write_at(self, payload, offset):
        if hasattr(os, "pwrite"):
            written = os.pwrite(self.fd, payload, offset)
            if written != len(payload):
                raise OSError("Gravação parcial do bloco")
            return
        with self.disk_lock:
            os.lseek(self.fd, offset, os.SEEK_SET)
            written = os.write(self.fd, payload)
            if written != len(payload):
                raise OSError("Gravação parcial do bloco")

    async def handle_client(self, reader, writer):
        try:
            while True:
                try:
                    request, _ = await receive_frame(reader)
                except (asyncio.IncompleteReadError, ConnectionError):
                    break
                kind = request.get("type")
                if kind == "META":
                    if self.meta is None:
                        await send_frame(writer, {"type": "NOT_READY"})
                    else:
                        await send_frame(writer, {"type": "META", "manifest": self.meta})
                elif kind == "HAVE":
                    if self.meta is None:
                        await send_frame(writer, {"type": "NOT_READY"})
                    else:
                        await send_frame(writer, {"type": "HAVE", "bitmap":
                                         base64.b64encode(bytes(self.have)).decode("ascii")})
                elif kind == "GET":
                    index = request.get("index")
                    if (self.meta is None or type(index) is not int or index < 0 or
                            index >= self.meta["block_count"] or
                            not self.have[index // 8] & (1 << (index % 8))):
                        await send_frame(writer, {"type": "NOT_AVAILABLE", "index": index})
                        continue
                    length = min(self.meta["block_size"],
                                 self.meta["file_size"] - index * self.meta["block_size"])
                    chunk = await asyncio.to_thread(
                        self.read_at, length, index * self.meta["block_size"])
                    await send_frame(writer, {"type": "BLOCK", "index": index,
                                              "source": self.name}, chunk)
                    self.log("BLOCK_SENT", target=request.get("requester", "?"), index=index,
                             bytes=len(chunk))
                else:
                    await send_frame(writer, {"type": "ERROR", "message": "Comando desconhecido"})
        except (ConnectionError, BrokenPipeError, asyncio.CancelledError):
            pass
        except Exception as exc:
            self.log("ERROR", stage="server", error=str(exc))
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def request_metadata(self):
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            for host, port in self.args.neighbor:
                try:
                    reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), 2)
                    try:
                        await asyncio.wait_for(send_frame(writer, {"type": "META"}), 5)
                        response, _ = await asyncio.wait_for(receive_frame(reader), NETWORK_TIMEOUT)
                        if response.get("type") == "META":
                            self.set_metadata(response["manifest"])
                            self.log("METADATA_READY", role="leecher", source=f"{host}:{port}",
                                     blocks=self.meta["block_count"],
                                     file_size=self.meta["file_size"], block_size=self.meta["block_size"])
                            return
                    finally:
                        writer.close()
                        await writer.wait_closed()
                except (OSError, ValueError, KeyError, asyncio.TimeoutError, ConnectionError) as exc:
                    self.log("NEIGHBOR_RETRY", source=f"{host}:{port}", error=str(exc))
            await asyncio.sleep(0.15)
        raise TimeoutError("Nenhum vizinho forneceu metadados dentro do prazo")

    async def reserve_block(self, remote, cursor):
        async with self.lock:
            size = len(self.have)
            for jump in range(size):
                pos = (cursor + jump) % size
                mask = remote[pos] & (~(self.have[pos] | self.reserved[pos]) & 255)
                if mask:
                    bit = (mask & -mask).bit_length() - 1
                    index = pos * 8 + bit
                    if index < self.meta["block_count"]:
                        self.reserved[pos] |= 1 << bit
                        return index, (pos + 1) % size
        return None, cursor

    async def release_block(self, index):
        async with self.lock:
            self.reserved[index // 8] &= ~(1 << (index % 8)) & 255

    async def accept_block(self, index, payload, source):
        expected_len = min(self.meta["block_size"],
                           self.meta["file_size"] - index * self.meta["block_size"])
        if len(payload) != expected_len or hashlib.sha256(payload).hexdigest() != self.meta["block_sha256"][index]:
            self.log("BLOCK_REJECTED", index=index, source=source, reason="SHA-256/tamanho inválido")
            return False
        await asyncio.to_thread(self.write_at, payload, index * self.meta["block_size"])
        async with self.lock:
            self.have[index // 8] |= 1 << (index % 8)
            self.blocks_received += 1
            self.sources[source] += 1
            if self.blocks_received == self.meta["block_count"]:
                self.finished.set()
        self.log("BLOCK_RECEIVED", index=index, source=source, bytes=len(payload),
                 downloaded=self.blocks_received, total=self.meta["block_count"])
        return True

    async def download_worker(self, host, port):
        target = f"{host}:{port}"
        cursor = int.from_bytes(hashlib.sha256(f"{self.name}-{port}".encode()).digest()[:4], "big") % len(self.have)
        while not self.finished.is_set():
            index = None
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), 3)
                self.log("NEIGHBOR_CONNECTED", target=target)
                try:
                    while not self.finished.is_set():
                        await asyncio.wait_for(send_frame(writer, {"type": "HAVE"}), NETWORK_TIMEOUT)
                        response, _ = await asyncio.wait_for(receive_frame(reader), NETWORK_TIMEOUT)
                        if response.get("type") != "HAVE":
                            await asyncio.sleep(0.08)
                            continue
                        remote = base64.b64decode(response["bitmap"], validate=True)
                        if len(remote) != len(self.have):
                            raise ValueError("Bitmap de vizinho com tamanho inválido")
                        index, cursor = await self.reserve_block(remote, cursor)
                        if index is None:
                            await asyncio.sleep(0.035)
                            continue
                        try:
                            await asyncio.wait_for(send_frame(writer, {"type": "GET",
                                                                       "index": index,
                                                                       "requester": self.name}), NETWORK_TIMEOUT)
                            answer, payload = await asyncio.wait_for(receive_frame(reader), NETWORK_TIMEOUT)
                            if answer.get("type") == "BLOCK" and answer.get("index") == index:
                                await self.accept_block(index, payload, target)
                            else:
                                self.log("BLOCK_UNAVAILABLE", index=index, source=target)
                        finally:
                            await self.release_block(index)
                            index = None
                finally:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except OSError:
                        pass
            except (OSError, ConnectionError, ValueError, asyncio.TimeoutError,
                    asyncio.IncompleteReadError) as exc:
                self.log("NEIGHBOR_RETRY", target=target, error=str(exc))
                if index is not None:
                    await self.release_block(index)
                await asyncio.sleep(0.12)

    async def start(self):
        if self.is_seed:
            self.load_seed()
        self.server = await asyncio.start_server(
            self.handle_client, self.args.listen_ip, self.args.port)
        self.log("READY", endpoint=f"{self.args.listen_ip}:{self.args.port}",
                 role="seed" if self.is_seed else "leecher",
                 neighbors=[f"{h}:{p}" for h, p in self.args.neighbor])
        try:
            if self.is_seed:
                await self.server.serve_forever()
                return
            await self.request_metadata()
            self.temp_path = self.dir / (self.meta["filename"] + ".part")
            self.fd = os.open(self.temp_path, os.O_CREAT | os.O_TRUNC | os.O_RDWR | BINARY_FLAG, 0o644)
            os.ftruncate(self.fd, self.meta["file_size"])
            self.download_start = time.monotonic()
            tasks = [asyncio.create_task(self.download_worker(host, port))
                     for host, port in self.args.neighbor]
            try:
                await asyncio.wait_for(self.finished.wait(), timeout=self.args.timeout)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            os.fsync(self.fd)
            calculated = await asyncio.to_thread(sha256_file, self.temp_path)
            expected = self.meta["file_sha256"]
            if calculated != expected:
                raise ValueError(f"SHA-256 final divergente: {calculated} != {expected}")
            output_path = self.dir / self.meta["filename"]
            await asyncio.to_thread(shutil.copyfile, self.temp_path, output_path)
            if os.path.getsize(output_path) != self.meta["file_size"]:
                raise ValueError("Tamanho do arquivo remontado inválido")
            elapsed = time.monotonic() - self.download_start
            status = {"peer": self.name, "complete": True,
                      "file": str(output_path), "bytes": self.meta["file_size"],
                      "block_size": self.meta["block_size"], "blocks": self.blocks_received,
                      "sha256": calculated, "expected_sha256": expected,
                      "seconds": round(elapsed, 4),
                      "sources": dict(self.sources)}
            (self.dir / "status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
            self.log("TRANSFER_COMPLETE", **status)
            if self.args.serve_seconds > 0:
                await asyncio.sleep(self.args.serve_seconds)
        finally:
            self.server.close()
            await self.server.wait_closed()
            if self.fd is not None:
                os.close(self.fd)
            self.log_file.close()


def main():
    parser = argparse.ArgumentParser(description="Peer para transferência TCP P2P por blocos")
    parser.add_argument("--name", required=True, help="Identificador do peer, ex.: A")
    parser.add_argument("--listen-ip", default="127.0.0.1", help="IP de escuta (127.0.0.1 local)")
    parser.add_argument("--port", required=True, type=int, help="Porta TCP local")
    parser.add_argument("--neighbor", action="append", type=decode_neighbor, default=[],
                        metavar="IP:PORTA", help="Vizinho estático; repetir conforme necessário")
    parser.add_argument("--data-dir", required=True, help="Pasta exclusiva deste peer")
    parser.add_argument("--seed-file", help="Arquivo original, somente no seeder inicial")
    parser.add_argument("--block-size", type=int, default=1024, help="Tamanho de bloco do seeder")
    parser.add_argument("--timeout", type=float, default=240, help="Prazo do download em segundos")
    parser.add_argument("--serve-seconds", type=float, default=40,
                        help="Tempo de compartilhamento após concluir o download")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.block_size < 1:
        parser.error("Porta e tamanho de bloco devem ser positivos e válidos")
    if not args.seed_file and not args.neighbor:
        parser.error("Um leecher precisa de pelo menos um --neighbor")
    try:
        asyncio.run(Peer(args).start())
    except KeyboardInterrupt:
        print("Peer encerrado pelo usuário")
    except Exception as exc:
        print(f"ERRO: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
