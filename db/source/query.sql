-- name: ListTerritoriesAfter :many
-- Paginação por keyset (WHERE id > último lido): custo constante por
-- lote, ao contrário de OFFSET, que relê as linhas já puladas.
SELECT st.territoryid,
       st.name   AS region,
       cr.name   AS country,
       st."group"
FROM sales.salesterritory st
JOIN person.countryregion cr
  ON cr.countryregioncode = st.countryregioncode
WHERE st.territoryid > sqlc.arg(after_id)::int
ORDER BY st.territoryid
LIMIT sqlc.arg(batch_size)::int;
