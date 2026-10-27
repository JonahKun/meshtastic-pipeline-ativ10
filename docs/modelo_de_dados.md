# Modelo de dados

DDL completo: `schema/mariadb.sql`. Todas as tabelas são InnoDB (transações são essenciais para a garantia de reinício), com prefixo `jonah_`. Todas as datas estão em **UTC**.

## Entrada (somente leitura): `pacotes_brutos` (MongoDB)

`{_id, topico, recebido_em: datetime UTC, mensagem: <JSON Meshtastic>}` ou `{..., mensagem_raw: str}` quando o JSON era inválido. Campos lidos de `mensagem`: `from`, `to`, `id`, `sender`, `type`, `timestamp`, `hops_away`, `rssi`, `snr`, `channel`, `payload`.

Esquema legado (`iot.sensores`, consumidor da turma): `{topic, received_at, payload (string), payload_data (objeto), data}`. O adaptador (`pipeline/adaptador.py`) o converte para o contrato acima; `jonah_serie.hop_start` guarda o `hop_start` do pacote junto de `hops_away`.

## Tabelas MariaDB

| Tabela | Chave | Conteúdo |
|---|---|---|
| `jonah_pacotes_vistos` | PK `(no, pacote_id)` | Um registro por pacote físico: `bruto_id` da primeira cópia, `gateway` (`sender`), `tipo`, `recebido_em`. |
| `jonah_duplicatas` | PK `bruto_id` | Cópias extras do mesmo `(no, pacote_id)` que chegaram por outro gateway/rota. |
| `jonah_serie` | PK `(no, pacote_id, metrica)` | Série limpa: `instante` (= `recebido_em`), `metrica`, `valor`, `instante_no`, `instante_no_valido`, `hops_away`, `rssi`, `snr`. |
| `jonah_agg_hora`, `jonah_agg_dia` | PK `(no, metrica, janela)` | `minimo`, `maximo`, `media`, `n` por janela (início da hora/dia em UTC). |
| `jonah_quarentena` | PK `bruto_id` | `motivo`, `detalhe`, `recebido_em`, `documento` (JSON original). |
| `jonah_contadores` | PK `nome` | Contadores cumulativos (ver abaixo). |
| `jonah_checkpoint` | PK `nome` | Posição de leitura: `recebido_em` + `ultimo_id`. |
| `jonah_nos` | PK `no` | Cadastro do nó vindo de `nodeinfo`: `nome_curto`, `nome_longo`, `hardware`, `papel`, `atualizado_em` (só é substituído por pacote mais novo). Redis espelha em `jonah:no:<no>`. |
| `jonah_lacunas` | PK `(no, inicio)` | Intervalos entre pacotes consecutivos > 2 × intervalo esperado. |

### Métricas (`jonah_serie.metrica`)

Nomes canônicos iguais aos do Meshtastic. Dispositivo: `battery_level` (101 = alimentado externamente), `voltage`, `channel_utilization`, `air_util_tx`, `uptime_seconds`. Ambiente: `temperature`, `relative_humidity`, `barometric_pressure`, `gas_resistance`, `iaq`, `lux`, `soil_moisture`, `soil_temperature`. Variações de nome entre versões (`batteryLevel`, `airUtilTx`, ...) são normalizadas em `pipeline/transformar.py`.

### Contadores (`jonah_contadores.nome`)

`processados`, `validos`, `duplicatas`, `timestamp_invalido` (+ `timestamp_invalido:ausente|antes_2024|futuro`), `quarentena` (+ `quarentena:<motivo>`), `sem_metrica`, `medidas`, `tipo:<tipo>`. Invariante: `processados = validos + duplicatas + quarentena`.

## Redis

| Chave | Tipo | Conteúdo |
|---|---|---|
| `jonah:ultimo:<no>:<metrica>` | hash | `valor`, `instante` (epoch em ms, usado na comparação), `instante_iso` (legível) |
| `jonah:nos` | set | nós já vistos com dado |

## Justificativas

- **Chave de dedup `(from, id)`, não o conteúdo.** O enunciado manda: o mesmo pacote chega por vários gateways, com `rssi`, `snr`, `hops_away` e `sender` diferentes, então o conteúdo difere para o mesmo pacote. E o contrário também ocorre: leituras de tensão idênticas, em momentos diferentes, são medições distintas. A identidade do pacote é o par nó de origem + id gerado pelo nó. `no` é texto `!aabbccdd` (formato do Meshtastic, legível), `pacote_id` é `BIGINT UNSIGNED` (uint32 cabe com folga).
- **`bruto_id` em `pacotes_vistos` e tabela `duplicatas`.** A releitura (sobreposição, reinício) precisa ser distinguível de uma duplicata real. Sem isso o contador de duplicatas cresceria a cada releitura. Com o `bruto_id` da primeira cópia e cada cópia extra registrada pelo próprio `bruto_id`, o contador é exato. Bônus: dá para ver quantos gateways ouviram cada pacote.
- **Tempo de referência = `recebido_em`.** Nós Meshtastic sem GPS/NTP têm RTC errado (zero, 1970, futuro). O horário de recebimento no servidor é a única fonte confiável e monotônica. O `timestamp` do nó é preservado (`instante_no`) com a flag `instante_no_valido`, para auditoria e para quem quiser usá-lo quando válido. Regra de validade: ausente/0, `< 2024-01-01` ou `> recebido_em + 5 min`.
- **Série longa (uma linha por métrica), não larga.** Tipos de telemetria variam por firmware e sensor (e virão sensores agrícolas). Uma tabela larga exigiria `ALTER TABLE` a cada métrica nova; a longa só ganha valores novos em `metrica`. PK `(no, pacote_id, metrica)` torna a gravação idempotente. Índices `(no, metrica, instante)` (séries de um nó) e `(metrica, instante)` (consultas da rede toda por período).
- **`valor DOUBLE`.** Todas as métricas são numéricas; DOUBLE evita erro de conversão e simplifica agregados. `instante` com `DATETIME(3)` (precisão do Mongo, milissegundos) em UTC, sem fuso implícito.
- **Agregados materializados por hora e dia**, recalculados por janela afetada com upsert (min, max, média, n). Recalcular a janela inteira a partir da série é idempotente e correto com dado tardio, ao contrário de somar incrementalmente (que quebraria com releitura). Guardar `n` permite recombinar médias com peso (consulta 2). Colunas `minimo`/`maximo` evitam colidir com nomes de função.
- **Quarentena em tabela, com documento original.** Nada é descartado: cada rejeição tem motivo categórico (contável por `GROUP BY motivo`), detalhe legível e o documento completo para reprocessar depois de corrigir a regra. PK em `bruto_id` torna idempotente.
- **Contadores em tabela, na mesma transação.** Um contador fora da transação poderia divergir dos dados após uma queda. Incrementar só quando o `INSERT IGNORE` insere de fato (`rowcount = 1`) mantém o invariante.
- **Checkpoint em `recebido_em` + `_id`.** `recebido_em` tem resolução de milissegundos e pode repetir; o `_id` desempata e ordena paginação de forma total. O checkpoint nunca regride. A releitura com sobreposição cobre gravações tardias.
- **Lacunas como tabela e como consulta.** A ausência é dado: a tabela `jonah_lacunas` guarda cada intervalo > 2 × intervalo esperado (configurável, padrão 15 min), recalculado ao redor de pacotes tardios; a consulta 3 mostra o silêncio atual (que ainda não virou lacuna por não ter terminado) e o maior intervalo. Nada é interpolado nem preenchido.
- **Redis só para "último valor".** Leitura O(1) para dashboard e alertas, sem varrer o MariaDB. Hash com `instante` em epoch ms permite a comparação numérica; a atualização condicional (`WATCH`) impede que um pacote antigo, chegado depois, regrida o valor. É dado derivado: se o Redis for perdido, reconstrói-se do MariaDB (a fonte da verdade).
- **Prefixo `jonah_` / `jonah:`** exigido pela disciplina, configurável por `PREFIXO` (o DDL é gerado com o prefixo escolhido).
