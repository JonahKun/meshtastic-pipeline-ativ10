-- Consulta 1: tensao media da bateria por no, por dia (dias em UTC).
-- Le o agregado diario (jonah_agg_dia), que o pipeline recalcula por janela afetada.
-- Compativel com MariaDB 10.6+.
SELECT
  no,
  DATE(janela)       AS dia,
  ROUND(media, 3)    AS tensao_media_v,
  ROUND(minimo, 3)   AS tensao_min_v,
  ROUND(maximo, 3)   AS tensao_max_v,
  n                  AS leituras
FROM jonah_agg_dia
WHERE metrica = 'voltage'
ORDER BY dia DESC, no
