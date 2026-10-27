-- Consulta 3: tempo de silencio por no. A AUSENCIA de pacotes como informacao (sem interpolar).
--   silencio_atual_min : minutos desde o ultimo pacote ate agora (UTC)
--   maior_intervalo_min: maior intervalo entre dois pacotes consecutivos do no
--   lacunas            : quantos intervalos passaram de 2x o intervalo esperado (jonah_lacunas)
-- Usa funcao de janela LAG (MariaDB 10.2+) e CTE. Compativel com MariaDB 10.6+.
-- Base: jonah_pacotes_vistos (um registro por pacote fisico, qualquer tipo), com recebido_em.
WITH intervalos AS (
  SELECT
    no,
    recebido_em,
    TIMESTAMPDIFF(SECOND,
                  LAG(recebido_em) OVER (PARTITION BY no ORDER BY recebido_em),
                  recebido_em) AS intervalo_s
  FROM jonah_pacotes_vistos
)
SELECT
  i.no,
  COUNT(*)                                                           AS pacotes,
  MAX(i.recebido_em)                                                 AS ultimo_pacote_utc,
  ROUND(TIMESTAMPDIFF(SECOND, MAX(i.recebido_em), UTC_TIMESTAMP()) / 60, 1) AS silencio_atual_min,
  ROUND(MAX(i.intervalo_s) / 60, 1)                                  AS maior_intervalo_min,
  (SELECT COUNT(*) FROM jonah_lacunas l WHERE l.no = i.no)           AS lacunas
FROM intervalos i
GROUP BY i.no
ORDER BY silencio_atual_min DESC
