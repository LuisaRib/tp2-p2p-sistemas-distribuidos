#!/usr/bin/env python3
"""Verificações isoladas do protocolo, metadados e integridade de blocos."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace

from peer import Peer, create_manifest, receive_frame, send_frame, validate_manifest


class UnitTests(unittest.TestCase):
    def test_manifest_and_fragments(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "arquivo.bin"
            file.write_bytes(b"1234567890")
            metadata = create_manifest(file, 4)
            self.assertEqual(metadata["block_count"], 3)
            self.assertEqual(metadata["block_size"], 4)
            self.assertEqual(metadata["file_size"], 10)
            validate_manifest(metadata)

    def test_reject_file_path_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "original.bin"
            file.write_bytes(b"1234")
            metadata = create_manifest(file, 4)
            metadata["filename"] = "../invalido.bin"
            with self.assertRaises(ValueError):
                validate_manifest(metadata)

    def test_reject_corrupted_block(self):
        async def exercise():
            with tempfile.TemporaryDirectory() as directory:
                file = Path(directory) / "origem.bin"
                file.write_bytes(b"abcdefgh")
                args = SimpleNamespace(name="T", data_dir=directory, seed_file=None,
                                       listen_ip="127.0.0.1", port=9999, neighbor=[])
                peer = Peer(args)
                peer.set_metadata(create_manifest(file, 4))
                target = Path(directory) / "partial.bin"
                peer.fd = os.open(target, os.O_CREAT | os.O_RDWR)
                os.ftruncate(peer.fd, 8)
                try:
                    self.assertFalse(await peer.accept_block(0, b"WXYZ", "invalido"))
                    self.assertEqual(peer.blocks_received, 0)
                    self.assertTrue(await peer.accept_block(0, b"abcd", "valido"))
                    self.assertEqual(peer.blocks_received, 1)
                    self.assertEqual(peer.read_at(4, 0), b"abcd")
                finally:
                    os.close(peer.fd)
                    peer.log_file.close()
        asyncio.run(exercise())

    def test_seed_preserves_all_binary_bytes(self):
        # Impede conversões de CRLF e Ctrl+Z ao ler arquivos no Windows.
        with tempfile.TemporaryDirectory() as directory:
            origin = Path(directory) / "dados.bin"
            payload = bytes(range(256)) * 8
            origin.write_bytes(payload)
            args = SimpleNamespace(name="S", data_dir=str(Path(directory) / "peer_S"),
                                   seed_file=str(origin), block_size=1024)
            peer = Peer(args)
            try:
                peer.load_seed()
                self.assertEqual(peer.read_at(len(payload), 0), payload)
            finally:
                if peer.fd is not None:
                    os.close(peer.fd)
                peer.log_file.close()

    def test_tcp_message_framing(self):
        async def exercise():
            async def handler(reader, writer):
                header, payload = await receive_frame(reader)
                self.assertEqual(header["type"], "HELLO")
                self.assertEqual(payload, b"dados")
                await send_frame(writer, {"type": "OK"}, b"resposta")
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_server(handler, "127.0.0.1", 0)
            try:
                port = server.sockets[0].getsockname()[1]
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                try:
                    await send_frame(writer, {"type": "HELLO"}, b"dados")
                    header, payload = await receive_frame(reader)
                    self.assertEqual(header["type"], "OK")
                    self.assertEqual(payload, b"resposta")
                finally:
                    writer.close()
                    await writer.wait_closed()
            finally:
                server.close()
                await server.wait_closed()
        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main(verbosity=2)
