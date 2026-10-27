-- Esquema do pipeline da ativ. 10 (IAL220, Fatec Pompeia). MariaDB 10.6+.
-- Todas as tabelas usam o prefixo obrigatorio jonah_ e InnoDB (transacoes).
-- Datas em UTC. Este arquivo e idempotente (CREATE TABLE IF NOT EXISTS) e e
-- executado na partida do pipeline. Nao use ponto e virgula dentro de comentarios.
-- Justificativas de cada escolha: docs/modelo_de_dados.md

-- Dedup: um registro por pacote fisico (no de origem, id do pacote), nao importa
-- por quantos gateways ele chegou. bruto_id guarda QUAL documento bruto o
-- registrou primeiro (distingue releitura de duplicata real).
CREATE TABLE IF NOT EXISTS jonah_pacotes_vistos (
  no           VARCHAR(16)  NOT NULL,
  pacote_id    BIGINT UNSIGNED NOT NULL,
  bruto_id     VARCHAR(64)  NOT NULL,
  gateway      VARCHAR(16)  NULL,
  tipo         VARCHAR(24)  NOT NULL,
  recebido_em  DATETIME(3)  NOT NULL,
  PRIMARY KEY (no, pacote_id),
  KEY ix_vistos_no_recebido (no, recebido_em),
  KEY ix_vistos_bruto (bruto_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Copias do mesmo pacote que chegaram por outro gateway (ou outra rota).
-- Registrar cada copia pelo bruto_id torna a contagem de duplicatas exata
-- mesmo quando o pipeline relê documentos (sobreposicao/reinicio).
CREATE TABLE IF NOT EXISTS jonah_duplicatas (
  bruto_id    VARCHAR(64)  NOT NULL,
  no          VARCHAR(16)  NOT NULL,
  pacote_id   BIGINT UNSIGNED NOT NULL,
  gateway     VARCHAR(16)  NULL,
  recebido_em DATETIME(3)  NOT NULL,
  PRIMARY KEY (bruto_id),
  KEY ix_duplicatas_pacote (no, pacote_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Serie limpa: uma linha por (pacote, metrica). instante = recebido_em.
CREATE TABLE IF NOT EXISTS jonah_serie (
  no                 VARCHAR(16)  NOT NULL,
  pacote_id          BIGINT UNSIGNED NOT NULL,
  metrica            VARCHAR(32)  NOT NULL,
  instante           DATETIME(3)  NOT NULL,
  valor              DOUBLE       NOT NULL,
  instante_no        DATETIME(3)  NULL,
  instante_no_valido TINYINT(1)   NOT NULL DEFAULT 0,
  hops_away          TINYINT      NULL,
  hop_start          TINYINT      NULL,
  rssi               SMALLINT     NULL,
  snr                DOUBLE       NULL,
  PRIMARY KEY (no, pacote_id, metrica),
  KEY ix_serie_no_metrica_instante (no, metrica, instante),
  KEY ix_serie_metrica_instante (metrica, instante)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS jonah_agg_hora (
  no      VARCHAR(16) NOT NULL,
  metrica VARCHAR(32) NOT NULL,
  janela  DATETIME    NOT NULL,
  minimo  DOUBLE      NOT NULL,
  maximo  DOUBLE      NOT NULL,
  media   DOUBLE      NOT NULL,
  n       INT         NOT NULL,
  PRIMARY KEY (no, metrica, janela),
  KEY ix_agg_hora_metrica_janela (metrica, janela)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS jonah_agg_dia (
  no      VARCHAR(16) NOT NULL,
  metrica VARCHAR(32) NOT NULL,
  janela  DATETIME    NOT NULL,
  minimo  DOUBLE      NOT NULL,
  maximo  DOUBLE      NOT NULL,
  media   DOUBLE      NOT NULL,
  n       INT         NOT NULL,
  PRIMARY KEY (no, metrica, janela),
  KEY ix_agg_dia_metrica_janela (metrica, janela)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Quarentena: o que nao passou na validacao, com motivo e o documento original.
CREATE TABLE IF NOT EXISTS jonah_quarentena (
  bruto_id    VARCHAR(64)  NOT NULL,
  motivo      VARCHAR(64)  NOT NULL,
  detalhe     VARCHAR(500) NULL,
  recebido_em DATETIME(3)  NULL,
  documento   LONGTEXT     NOT NULL,
  PRIMARY KEY (bruto_id),
  KEY ix_quarentena_motivo (motivo, recebido_em)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Contadores cumulativos, atualizados na mesma transacao dos dados.
CREATE TABLE IF NOT EXISTS jonah_contadores (
  nome  VARCHAR(80) NOT NULL,
  valor BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (nome)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Checkpoint de leitura do Mongo (recebido_em, com _id como desempate).
CREATE TABLE IF NOT EXISTS jonah_checkpoint (
  nome          VARCHAR(40) NOT NULL,
  recebido_em   DATETIME(3) NOT NULL,
  ultimo_id     VARCHAR(64) NOT NULL,
  atualizado_em DATETIME(3) NOT NULL,
  PRIMARY KEY (nome)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Cadastro dos nos, alimentado pelos pacotes nodeinfo (nome curto/longo, hardware, papel).
CREATE TABLE IF NOT EXISTS jonah_nos (
  no            VARCHAR(16) NOT NULL,
  nome_curto    VARCHAR(64) NULL,
  nome_longo    VARCHAR(64) NULL,
  hardware      INT         NULL,
  papel         INT         NULL,
  atualizado_em DATETIME(3) NOT NULL,
  PRIMARY KEY (no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Lacunas: ausencia de pacotes como informacao (nunca interpolada).
-- Um intervalo entre dois pacotes consecutivos de um no, maior que
-- 2 x INTERVALO_ESPERADO_MIN, vira uma linha aqui.
CREATE TABLE IF NOT EXISTS jonah_lacunas (
  no        VARCHAR(16) NOT NULL,
  inicio    DATETIME(3) NOT NULL,
  fim       DATETIME(3) NOT NULL,
  duracao_s INT         NOT NULL,
  PRIMARY KEY (no, inicio),
  KEY ix_lacunas_duracao (duracao_s)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

