WITH ak_names AS
  (SELECT DISTINCT cu.id AS user_id,
          LISTAGG(DISTINCT uf.value, ',') WITHIN GROUP (ORDER BY uf.value) AS voter_names
   FROM ak_moveon.core_user cu
   JOIN ak_moveon.core_action ca ON ca.user_id = cu.id
   JOIN stafftemp.latest_phone lp ON lp.akid = cu.id
   JOIN reach.tmc__mvo_users ru ON ru.phonenumber = lp.mobile
   LEFT JOIN ak_moveon.core_userfield uf ON uf.parent_id = cu.id
   AND uf.name = 'reach_voters_all'
   WHERE ca.page_id = 33039
   GROUP BY 1),
reach_names AS
  (SELECT DISTINCT cu.id AS user_id,
          LISTAGG(DISTINCT rr.personfirstname || ' ' || rr.personlastname, ',') WITHIN GROUP (ORDER BY rr.personfirstname || ' ' || rr.personlastname) AS voter_names
   FROM ak_moveon.core_user cu
   JOIN ak_moveon.core_action ca ON ca.user_id = cu.id
   JOIN stafftemp.latest_phone lp ON lp.akid = cu.id
   JOIN reach.tmc__mvo_users ru ON ru.phonenumber = lp.mobile
   LEFT JOIN reach.tmc__mvo_relationships rr ON rr.userid = ru.userid
   WHERE ca.page_id = 33039
   GROUP BY 1)
SELECT a.user_id,
       r.voter_names
FROM ak_names a
JOIN reach_names r ON r.user_id = a.user_id
WHERE r.voter_names IS NOT NULL
  AND (a.voter_names IS NULL
       OR a.voter_names != r.voter_names)