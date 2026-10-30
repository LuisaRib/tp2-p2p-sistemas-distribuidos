#!/usr/bin/env python3
"""Testes automatizados com 2/4 processos P2P e comparação de SHA-256."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

PROJECT_DIR = Path(__file__).resolve().parent

# Todos os valores de KB/MB do roteiro são interpretados em base 1024.
CASES = [
    ("A_10KB", 10 * 1024, 2, 1024),
    ("A_10KB", 10 * 1024, 4, 1024),
    ("A_20KB", 20 * 1024, 2, 4096),
    ("A_20KB", 20 * 1024, 4, 4096),
    ("B_1MB", 1 * 1024**2, 2, 1024),
    ("B_1MB", 1 * 1024**2, 4, 1024),
    ("B_5MB", 5 * 1024**2, 2, 4096),
    ("B_5MB", 5 * 1024**2, 4, 4096),
    ("C_10MB", 10 * 1024**2, 2, 1024),
    ("C_10MB", 10 * 1024**2, 4, 1024),
    ("C_20MB", 20 * 1024**2, 2, 4096),
    ("C_20MB", 20 * 1024**2, 4, 4096),
    ("B_1MB", 1 * 1024**2, 2, 4096),  # Controle: mesmo arquivo/peers, outro bloco
    ("B_1MB", 1 * 1024**2, 4, 4096),
]


def reserve_ports(n):
    sockets = []
    try:
        for _ in range(n):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            sockets.append(s)
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


def make_random_file(path, size):
    digest = hashlib.sha256()
    with open(path, "wb") as file:
        for _ in range(size // 1024**2):
            chunk = os.urandom(1024**2)
            file.write(chunk)
            digest.update(chunk)
        if size % 1024**2:
            chunk = os.urandom(size % 1024**2)
            file.write(chunk)
            digest.update(chunk)
    return digest.hexdigest()


def wait_port(port, process, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Seeder encerrou prematuramente: {process.returncode}")
        with socket.socket() as s:
            s.settimeout(0.1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.02)
    raise TimeoutError("Seeder não abriu a porta")


def wait_peer_ready(folder, name, process, timeout=15):
    deadline = time.monotonic() + timeout
    log = folder / f"peer_{name}" / "events.jsonl"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Peer {name} encerrou na inicialização")
        if log.exists():
            try:
                if '"event": "READY"' in log.read_text(encoding="utf-8"):
                    return
            except (OSError, UnicodeError):
                pass
        time.sleep(0.03)
    raise TimeoutError(f"Peer {name} não iniciou em {timeout} segundos")


def run_case(case, index, root, timeout):
    tag, size, count, block_size = case
    case_name = f"{index:02d}_{tag}_{count}peers_{block_size}bytes"
    folder = root / case_name
    folder.mkdir(parents=True, exist_ok=True)
    # O arquivo de teste muda a cada execução: jamais usar status/READY antigos.
    # Removemos apenas esses arquivos gerados, preservando os demais dados e logs.
    for name in (chr(ord("A") + i) for i in range(count)):
        peer_dir = folder / f"peer_{name}"
        for stale_name in ("status.json", "events.jsonl"):
            (peer_dir / stale_name).unlink(missing_ok=True)
    original = folder / "original.bin"
    original_sha = make_random_file(original, size)
    peers = [chr(ord("A") + i) for i in range(count)]
    ports = dict(zip(peers, reserve_ports(count)))
    # D não conhece A: necessita baixar blocos já obtidos por B/C.
    topology = ({"A": ["B"], "B": ["A"]} if count == 2 else
                {"A": ["B", "C"], "B": ["A", "C", "D"],
                 "C": ["A", "B", "D"], "D": ["B", "C"]})
    processes, log_handles = [], []
    start = time.monotonic()
    try:
        for name in peers:
            args = [sys.executable, "-u", str(PROJECT_DIR / "peer.py"),
                    "--name", name, "--port", str(ports[name]),
                    "--data-dir", str(folder / f"peer_{name}"),
                    "--timeout", str(timeout), "--serve-seconds", str(timeout)]
            if name == "A":
                args += ["--seed-file", str(original), "--block-size", str(block_size)]
            for neighbor in topology[name]:
                args += ["--neighbor", f"127.0.0.1:{ports[neighbor]}"]
            logfile = open(folder / f"peer_{name}.console.log", "w", encoding="utf-8")
            log_handles.append(logfile)
            proc = subprocess.Popen(args, cwd=PROJECT_DIR, stdout=logfile, stderr=subprocess.STDOUT)
            processes.append(proc)
            if name == "A":
                wait_port(ports[name], proc)
            else:
                wait_peer_ready(folder, name, proc)
        deadline = time.monotonic() + timeout
        completed = {}
        while time.monotonic() < deadline:
            for name, proc in zip(peers, processes):
                if proc.poll() not in (None, 0):
                    raise RuntimeError(f"Peer {name} falhou (exit={proc.returncode}); veja o console")
            for name in peers[1:]:
                status_path = folder / f"peer_{name}" / "status.json"
                if name not in completed and status_path.exists():
                    try:
                        completed[name] = json.loads(status_path.read_text(encoding="utf-8"))
                    except (json.JSONDecodeError, OSError):
                        pass
            if len(completed) == count - 1:
                break
            time.sleep(0.05)
        if len(completed) != count - 1:
            raise TimeoutError(f"Transferência não concluída: {peers[1:]}, concluídos={list(completed)}")
        elapsed = time.monotonic() - start
        for peer_name, status in completed.items():
            if status["sha256"] != original_sha or status["bytes"] != size:
                raise AssertionError(
                    f"Integridade/tamanho divergentes no Peer {peer_name}: "
                    f"SHA esperado={original_sha}, recebido={status['sha256']}; "
                    f"bytes esperados={size}, recebidos={status['bytes']}")
            if status["blocks"] != (size + block_size - 1) // block_size:
                raise AssertionError("Contagem de blocos divergente")
            if not Path(status["file"]).exists():
                raise AssertionError("Arquivo final ausente")
            # Checagem independente do checksum calculado pelo próprio peer.
            digest = hashlib.sha256()
            with open(status["file"], "rb") as downloaded:
                for chunk in iter(lambda: downloaded.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != original_sha:
                raise AssertionError("SHA-256 externo do arquivo final divergente")
        if count == 4:
            d_sources = completed["D"]["sources"]
            if not d_sources or any(source.endswith(f":{ports['A']}") for source in d_sources):
                raise AssertionError("O Peer D deveria receber somente de B/C")
        row = {"caso": case_name, "arquivo": tag, "bytes": size,
               "peers": count, "bloco_bytes": block_size,
               "total_blocos": (size + block_size - 1) // block_size,
               "tempo_total_s": round(elapsed, 4),
               "tempo_medio_download_s": round(sum(s["seconds"] for s in completed.values()) / len(completed), 4),
               "tempo_max_download_s": round(max(s["seconds"] for s in completed.values()), 4),
               "sha256_original": original_sha, "integridade": "OK",
               "origens_peer_D": json.dumps(completed.get("D", {}).get("sources", {})),
               "origens_por_peer": json.dumps({k: v["sources"] for k, v in completed.items()})}
        print(f"OK {case_name}: {elapsed:.3f}s; SHA-256 igual; "
              f"fontes D: {completed.get('D', {}).get('sources', {})}", flush=True)
        return row
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for handle in log_handles:
            handle.close()


def main():
    parser = argparse.ArgumentParser(description="Suíte de experimentos do TP2 P2P")
    parser.add_argument("--suite", choices=["quick", "full"], default="quick")
    parser.add_argument("--out", default=str(PROJECT_DIR / "resultados"),
                        help="Pasta para arquivos, logs e dados de teste")
    parser.add_argument("--only", type=int, help="Executar somente o caso N da suíte completa")
    parser.add_argument("--timeout", type=int, default=180,
                        help="Tempo-limite em segundos para cada teste")
    args = parser.parse_args()
    root = Path(args.out).resolve()
    root.mkdir(parents=True, exist_ok=True)
    selected = (CASES if args.suite == "full" else [CASES[0], CASES[5], CASES[11]])
    if args.only is not None:
        if not 1 <= args.only <= len(CASES):
            parser.error(f"--only deve estar entre 1 e {len(CASES)}")
        selected = [CASES[args.only - 1]]
    rows, errors = [], []
    for i, case in enumerate(selected, 1):
        print(f"INICIANDO caso {i}/{len(selected)}: arquivo={case[0]}, "
              f"peers={case[2]}, bloco={case[3]} bytes", flush=True)
        try:
            rows.append(run_case(case, i, root, args.timeout))
        except Exception as exc:
            print(f"FALHOU no caso {i}: {exc}", file=sys.stderr, flush=True)
            errors.append({"caso": i, "config": case, "error": str(exc)})
            # Apresenta um trecho dos logs para evitar testes silenciosos.
            case_dir = root / f"{i:02d}_{case[0]}_{case[2]}peers_{case[3]}bytes"
            for log in sorted(case_dir.glob("peer_*.console.log")):
                lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
                if lines:
                    print(f"  {log.name}: {lines[-1][:400]}", file=sys.stderr, flush=True)
        (root / "resumo.json").write_text(
            json.dumps({"casos": rows, "erros": errors}, indent=2, ensure_ascii=False),
            encoding="utf-8")
        if rows:
            with open(root / "resumo.csv", "w", encoding="utf-8-sig", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
    print(f"\nRESULTADO: {len(rows)}/{len(selected)} aprovações, {len(errors)} falhas.", flush=True)
    print(f"Arquivos, status, logs e CSV: {root}", flush=True)
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
