# RASCUNHO do artigo LinkedIn: Atividade 10

> Rascunho com marcadores. Tudo que está em `[PREENCHER: ...]` ou `[link ...]` deve ser substituído por dados REAIS da execução de 72 h. Não publique números que você não mediu. Apague esta nota antes de publicar.

**Título sugerido:** Do pacote bruto ao dado útil: o que aprendi construindo o pipeline da nossa rede LoRa

Marcações: Fatec Pompeia · Prof. Allan Siriani · #IAL220 #Meshtastic #LoRa #BigData #Python

---

Na rede Meshtastic que estamos montando em Pompeia, um pacote não chega "limpo". Ele chega repetido, com relógio errado, com buracos no meio e, às vezes, quebrado. Nesta atividade da disciplina de Projeto Integrador de Big Data (Fatec Pompeia, professor Allan Siriani) construí o pipeline que transforma o que o gateway grava no MongoDB em dado em que dá para confiar.

**O fluxo:** nós Meshtastic → gateway ESP32 → broker MQTT → MongoDB (bruto, só leitura) → pipeline em Python → MariaDB (série limpa e agregados) + Redis (último valor).

## As quatro decisões que importam

**1. Duplicata é pela identidade do pacote, nunca pelo conteúdo.**
O mesmo pacote chega por vários gateways, com RSSI e SNR diferentes. Duas leituras de tensão com o mesmo valor, em horas diferentes, são medições diferentes. Por isso a chave é o par (nó de origem, id do pacote), com `INSERT IGNORE` no MariaDB. [PREENCHER: duplicatas contadas em 72h]

**2. O relógio do nó não manda no tempo.**
Nós sem GPS/NTP mandam timestamp 0, 1970 ou do futuro. A referência é o horário em que o pacote chegou ao servidor. O timestamp do nó fica como atributo, marcado como inválido quando ausente, anterior a 2024 ou mais de 5 minutos à frente do recebimento. [PREENCHER: timestamps inválidos e motivos em 72h]

**3. Buraco é informação, não algo a preencher.**
Não interpolei nada. Tempo de silêncio por nó e as lacunas (intervalo maior que 2 vezes o esperado) viram dado: um nó mudo diz algo sobre bateria, alcance ou falha. [PREENCHER: nó com maior silêncio e o que você descobriu sobre ele]

**4. O que não bate não some: vai para a quarentena, com motivo.**
JSON inválido, tipo desconhecido, campo faltando, valor fora da faixa física. Cada rejeição guarda o motivo e o documento original, e é contável. [PREENCHER: quarentena por motivo em 72h]

## Como não duplicar nem perder ao reiniciar

Leitura "pelo menos uma vez" + escrita idempotente. O checkpoint avança na mesma transação MariaDB que grava o lote (série, vistos, contadores, agregados), então ou tudo vale ou nada vale. Releio uma janela de sobreposição e as chaves únicas absorvem a repetição; os contadores só sobem quando o `INSERT IGNORE` realmente inseriu. O Redis só atualiza se o instante for mais novo, e é reconstruído a partir do MariaDB se for perdido. [PREENCHER: quantas vezes o pipeline reiniciou nas 72h e se os contadores bateram]

## Modelo de dados

Série longa (uma linha por métrica) em vez de tabela larga, porque os tipos de telemetria mudam com o firmware e virão os sensores agrícolas. Agregados por hora e dia recalculados por janela afetada (idempotentes). Detalhes e justificativas no repositório (`docs/modelo_de_dados.md`).

## Resultados (72 h de operação)

- Período: [PREENCHER: início e fim reais da execução]
- Contadores finais: [PREENCHER: saída de `python relatorio_contadores.py`]
- Consulta 1, tensão média de bateria por nó por dia: [PREENCHER: resultado consulta 1]
- Consulta 2, utilização de canal por hora: [PREENCHER: resultado consulta 2]
- Consulta 3, tempo de silêncio por nó: [PREENCHER: resultado consulta 3]
- Onde o pipeline ficou rodando: [PREENCHER: servidor/container e como foi mantido ativo]

## O que deu errado e o que aprendi

[PREENCHER: problemas reais encontrados durante as 72 h e como foram resolvidos. Seja honesto, inclusive sobre atrasos.]

## Código

Repositório público: [link do repositório]

Obrigado ao professor Allan Siriani e à Fatec Pompeia pelo desafio.

@Fatec Pompeia @Allan Siriani
