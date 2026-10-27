-- Consulta 2: utilizacao do canal (channel_utilization, em %) por hora, na rede toda.
-- A media da rede e ponderada pelo numero de leituras de cada no (SUM(media*n)/SUM(n)),
-- o que equivale a media simples de todas as leituras da hora. Horas em UTC.
-- Compativel com MariaDB 10.6+.
SELECT
  janela                              AS hora_utc,
  COUNT(DISTINCT no)                  AS nos_com_dados,
  ROUND(SUM(media * n) / SUM(n), 2)   AS utilizacao_media_pct,
  ROUND(MAX(maximo), 2)               AS utilizacao_max_pct,
  SUM(n)                              AS leituras
FROM jonah_agg_hora
WHERE metrica = 'channel_utilization'
GROUP BY janela
ORDER BY janela DESC
