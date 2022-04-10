-- le chiffre d’affaire jour par jour, du 1er janvier 2019 au 31 décembre 2019
-- sur Bigquery
SELECT format_date("%d/%m/%Y",parse_date("%d/%m/%y",tr_date )) AS DATE, SUM(prod_price * prod_qty ) AS VENTES
FROM  client_tr
WHERE parse_date("%d/%m/%y",tr_date ) BETWEEN "2019-01-01" AND "2019-12-31"
GROUP BY tr_date
ORDER BY parse_date("%d/%m/%y",tr_date ) ASC;
