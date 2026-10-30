# Trabalho Prático 2 — Transferência de Arquivos Peer-to-Peer

**Disciplina:** Sistemas Distribuídos — CEFET-MG — 2026/2  
**Tema:** fragmentação e transferência P2P simétrica usando sockets TCP.

## 1. Pré-requisitos

- **Python 3.10 ou superior** (recomendado Python 3.11+). Verifique no terminal: `python --version`.
- Windows, Linux ou macOS.
- **Não são necessárias bibliotecas externas** (`pip install` não é necessário).
- As portas TCP escolhidas precisam estar livres. Em testes locais são usados endereços `127.0.0.1`.

## 2. Estrutura do projeto

```text
TP2_P2P_Sistemas_Distribuidos/
├── peer.py                 # Peer: servidor + cliente; protocolo e montagem
├── run_tests.py            # Testes integrados com 2 ou 4 processos
├── tests_unitarios.py      # Cinco testes unitários
├── resultados_medidos.csv  # Resultados realmente observados nesta execução
├── resultados_medidos.json # Mesmo resumo, com fontes por peer
├── relatorio_tp2.pdf       # Relatório em PDF (máximo 5 páginas)
├── relatorio_tp2.docx      # Versão editável para nome e URL GitHub
├── exemplos_logs/          # Extratos reais de logs JSONL
└── README.md               # Este passo a passo
```

## 3. Começando: testes automáticos (recomendado)

**Passo 1:** baixe e extraia o arquivo `.zip` com o projeto.

**Passo 2:** abra um terminal na pasta que contém `peer.py` e `run_tests.py`. No Windows, também é possível digitar `cmd` na barra de endereço dessa pasta e pressionar Enter.

**Passo 3:** confira o Python (no Windows, use `py` caso `python` não funcione):

```bash
python --version
```

**Passo 4:** execute os testes unitários (metadados, integridade, enquadramento de mensagens TCP):

```bash
python tests_unitarios.py
```

**Passo 5:** execute os testes rápidos (3 cenários):

```bash
python run_tests.py --suite quick
```

**Passo 6:** execute a bateria com 14 cenários, abrangendo 2/4 peers, arquivos A/B/C e blocos de 1024/4096 bytes:

```bash
python run_tests.py --suite full
```

> Os 14 testes geram arquivos de até 20 MiB e logs extensos. A execução pode durar alguns minutos, dependendo da máquina. Execute uma suíte por vez. A primeira bateria completa que acompanhei precisou ter três casos executados de forma isolada; todos foram aprovados após essa execução. Isso não é garantia de tempos idênticos em outros ambientes.

**Passo 7:** procure a pasta `resultados/` criada pelo script. Ela contém:

- `resumo.csv` e `resumo.json`: tempos, parâmetros, tamanho e SHA-256 do arquivo original, estado final e fontes.
- Subpastas de cada teste: `original.bin`, `peer_A/`, `peer_B/`, ...
- Cada peer: `metadata.json`, `events.jsonl` (logs) e, no caso de um leecher, `original.bin` remontado e `status.json`.
- `peer_X.console.log`: saída do processo com eventos de inicialização e conclusão.

**Passo 8:** abra `resumo.csv` (Excel ou editor de texto) e confira `integridade = OK` para os 14 testes. Os arquivos finais também podem ser comparados com `original.bin` por SHA-256.

## 4. Executando os peers manualmente

Na pasta do projeto, crie um arquivo aleatório de 10 KiB:

```bash
python -c "from pathlib import Path; import os; Path('arquivo.bin').write_bytes(os.urandom(10*1024))"
```

### Exemplo com 2 peers

Abra **dois terminais**, ambos posicionados na pasta do projeto. Deixe ambos abertos enquanto testa.

**Terminal 1 — Peer A (Seeder):**

```bash
python peer.py --name A --port 9101 --data-dir manual/A --seed-file arquivo.bin --block-size 1024 --neighbor 127.0.0.1:9102
```

**Terminal 2 — Peer B (Leecher):**

```bash
python peer.py --name B --port 9102 --data-dir manual/B --neighbor 127.0.0.1:9101
```

Aparecerá o evento `TRANSFER_COMPLETE` quando o Peer B concluir e verificar o download. O arquivo estará em `manual/B/arquivo.bin`. B continuará compartilhando por 40 segundos (configurável por `--serve-seconds`). Para encerrar A, pressione `Ctrl+C` no seu terminal.

### Exemplo com 4 peers — prova de P2P simétrico

Execute cada comando em um terminal separado. Inicie A, depois B e C, e por último D:

**Terminal 1 — A:**

```bash
python peer.py --name A --port 9101 --data-dir manual4/A --seed-file arquivo.bin --block-size 1024 --neighbor 127.0.0.1:9102 --neighbor 127.0.0.1:9103
```

**Terminal 2 — B:**

```bash
python peer.py --name B --port 9102 --data-dir manual4/B --neighbor 127.0.0.1:9101 --neighbor 127.0.0.1:9103 --neighbor 127.0.0.1:9104
```

**Terminal 3 — C:**

```bash
python peer.py --name C --port 9103 --data-dir manual4/C --neighbor 127.0.0.1:9101 --neighbor 127.0.0.1:9102 --neighbor 127.0.0.1:9104
```

**Terminal 4 — D:**

```bash
python peer.py --name D --port 9104 --data-dir manual4/D --neighbor 127.0.0.1:9102 --neighbor 127.0.0.1:9103
```

**Por que é importante?** D não tem A como vizinho. Portanto, para receber o arquivo, ele depende de blocos que B ou C já possuam e estejam compartilhando — inclusive antes de completar seus próprios downloads. Consulte `manual4/D/status.json`, campo `sources`, e `manual4/D/events.jsonl`, evento `BLOCK_RECEIVED`.

Se você repetir esse teste, pode apagar previamente as pastas `manual/` e `manual4/`. O download é reiniciado do zero, por decisão de escopo.

## 5. Protocolo e fragmentação

A comunicação é TCP e usa um protocolo próprio: `4 bytes` de tamanho de cabeçalho (inteiro sem sinal, big-endian) + `cabeçalho JSON` + `payload binário` opcional. O JSON contém o tamanho do payload.

| Tipo | Solicitação | Resposta |
|---|---|---|
| `META` | Pedir metadados | `META` com manifest JSON ou `NOT_READY` |
| `HAVE` | Quais blocos estão disponíveis? | `HAVE` com bitmap codificado em Base64 |
| `GET` | Solicitar bloco por índice | `BLOCK` com payload binário ou `NOT_AVAILABLE` |

O seeder gera um arquivo `metadata.json` com nome, tamanho do arquivo, tamanho/quantidade de blocos, SHA-256 do arquivo inteiro e SHA-256 individual de cada bloco.

Ao baixar, cada peer usa um bitmap local de posse de blocos, um bitmap de reservas para evitar pedidos duplicados e tarefas de download concorrentes para os vizinhos. Cada bloco é validado com SHA-256 antes de entrar no conjunto compartilhável. Quando todos chegam, o arquivo é validado novamente e gravado com o nome original.

## 6. Testes registrados no relatório

A bateria foi validada com **14 cenários**: arquivo A (10/20 KiB), B (1/5 MiB), C (10/20 MiB), dois ou quatro peers e blocos de 1024/4096 bytes, além de uma comparação controlada de tamanhos de bloco para o arquivo de 1 MiB. Todas as 14 configurações funcionaram no ambiente local de testes. Consulte `resultados_medidos.csv` e o PDF para os números realmente observados.

Não compare tempos de máquinas diferentes como se fossem benchmarks equivalentes. Em particular, os testes locais por `127.0.0.1` não representam transferência em rede física.

## 7. Entrega: tarefas restantes no computador do aluno

1. Execute `python tests_unitarios.py` e `python run_tests.py --suite full` no computador usado para a demonstração, para confirmar o funcionamento no ambiente da apresentação.
2. **Edite `relatorio_tp2.docx`:** informe o(s) autor(es) e substitua o campo da URL pelo link real do repositório. Exporte novamente como PDF. O relatório foi mantido em até cinco páginas.
3. Publique `peer.py`, `run_tests.py`, `tests_unitarios.py`, `README.md` e, se desejar, o resumo dos testes em um **repositório GitHub**. Não publique as pastas de testes grandes nem arquivos locais gerados. O enunciado exige a URL do código-fonte no relatório.
4. Entregue o relatório em PDF e a URL do código-fonte conforme o canal estabelecido pela disciplina.

## 8. Limitações declaradas

- Rede de vizinhos estática; não há tracker (opcional no enunciado).
- Um arquivo é transferido por execução do conjunto de peers.
- O download parcial não é retomado após reinicialização; os blocos são baixados novamente.
- Não há autenticação nem criptografia, pois o foco é o protocolo P2P elementar.
- Ao concluir, o leecher permanece servindo temporariamente (padrão: 40 segundos), para não interromper downloads dos outros peers.
- Os scripts criam arquivos aleatórios únicos em cada cenário; os tempos de testes repetidos podem variar.

**Observação:** `--serve-seconds 0` permite que o leecher encerre imediatamente, mas pode impedir que outros peers aproveitem seus blocos. Use o comportamento padrão para os testes P2P.

## 9. Observação de compatibilidade Windows

Esta versão inclui correção para leitura e escrita com `os.O_BINARY`. No Windows,
a ausência dessa opção pode converter bytes de arquivos binários, invalidando
o SHA-256 e impedindo a transferência. O novo teste unitário verifica essa situação.
No VS Code/PowerShell use `py tests_unitarios.py`, `py run_tests.py --suite quick`
e `py run_tests.py --suite full`.

## 10. Validação no Windows (30/10/2026)

A bateria rápida passou (3/3), assim como a bateria completa (14/14, zero falhas) no VS Code/PowerShell, conforme resultado informado pela pessoa que executou o projeto. A demonstração manual com quatro peers também foi concluída: o Peer D recebeu 513 blocos de B e 511 de C (1 MiB, 1024 blocos), em 0,7632 s, com SHA-256 correto. Consulte `evidencia_validacao_windows.md`.

**Para repetir a validação no Windows:**

```powershell
py tests_unitarios.py
py run_tests.py --suite quick --timeout 180
py run_tests.py --suite full --timeout 300
```

O relatório foi atualizado com os resultados do Windows; os tempos da tabela no relatório ainda são medições de referência executadas no Linux, e não os tempos individuais da máquina Windows.
