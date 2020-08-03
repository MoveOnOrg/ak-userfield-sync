WITH reach_account_info AS
  (SELECT DISTINCT cu.id AS user_id,
                   CASE
                       WHEN ru.userid IS NULL THEN 0
                       ELSE 1
                   END AS has_reach_account,
                   uf.value AS uf_value
   FROM ak_moveon.core_user cu
   JOIN ak_moveon.core_action ca ON ca.user_id = cu.id
   LEFT JOIN stafftemp.latest_phone lp ON lp.akid = cu.id
   LEFT JOIN reach.tmc__mvo_users ru ON ru.phonenumber = lp.mobile
   LEFT JOIN ak_moveon.core_userfield uf ON uf.parent_id = cu.id
   AND uf.name = 'has_reach_account'
   WHERE ca.page_id = 33039 )
SELECT user_id, has_reach_account
FROM reach_account_info
WHERE uf_value IS NULL
  OR uf_value != has_reach_account
ORDER BY RANDOM()
