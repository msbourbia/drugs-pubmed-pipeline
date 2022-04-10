-- les ventes meuble et déco par client réalisées entre 1er janvier 2019 et 31 décembre 2019.
-- sur Bigquery

client_trans_ptype AS (
    SELECT trans.client_id AS client_id, prod_nomct.product_type AS ptype, SUM(trans.prod_price*trans.prod_qty) AS ventes
    FROM TRANSACTION trans INNER JOIN PRODUCT_NOMENCLATURE prod_nomct
    ON trans.prop_id = prod_nomct.product_id
	WHERE parse_date("%d/%m/%y",trans.tr_date ) BETWEEN "2019-01-01" AND "2019-12-31"
    GROUP BY trans.client_id , prod_nomct.product_type
),
ventes_meuble AS (
    SELECT client_id, ventes
    FROM client_trans_ptype WHERE ptype = 'meuble'
),
ventes_deco AS (
    SELECT client_id, ventes
    FROM client_trans_ptype WHERE ptype = 'deco'
)
SELECT IFNULL(d.client_id, m.client_id) AS client_id,
        IFNULL(d.ventes,0) AS ventes_deco,
        IFNULL(m.ventes,0) AS ventes_meuble
FROM ventes_deco d  FULL JOIN ventes_meuble m ON d.client_id =m.client_id
;