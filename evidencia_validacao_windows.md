# Evidência de validação — Windows, VS Code

**Data:** 30/10/2026. Resultados informados pela pessoa que executou o programa no próprio computador.

## Testes automatizados

- Suíte rápida: **3/3 aprovações, 0 falhas**.
- Suíte completa: **14/14 aprovações, 0 falhas**.

## Demonstração manual com quatro peers

- Peer D: `127.0.0.1:9104`; vizinhos: B (`127.0.0.1:9102`) e C (`127.0.0.1:9103`).
- Evento final: `TRANSFER_COMPLETE`, `complete=true`.
- Arquivo recebido: `1048576` bytes (1 MiB); `1024` blocos de `1024` bytes.
- Tempo informado pelo programa: `0.7632` segundo.
- Origem: B = `513` blocos; C = `511` blocos.
- SHA-256 calculado e esperado: `a74d4b7699dd03f594a85e6521ecd7e360e7af383fc5b9d47bf688a999b5596a`.

**Interpretação:** o Peer D recebeu todos os blocos de B e C, sem conexão direta com o seeder inicial A; o arquivo final foi validado com sucesso.

**Nota:** esta evidência reproduz os resultados compartilhados durante a validação. Os arquivos de log gerados no Windows permanecem na pasta local `manual4/` e `resultados/` e não foram enviados para este pacote. Os tempos de `resultados_medidos.csv` correspondem a uma execução de referência distinta.
