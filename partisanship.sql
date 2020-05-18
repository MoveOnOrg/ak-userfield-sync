SELECT p.ak_user_id AS user_id,
       p.partisanship
FROM stafftemp.new_member_partisanship_since_march20200301 p
LEFT JOIN ak_moveon.core_userfield uf ON uf.parent_id = p.ak_user_id AND uf.name = 'partisanship'
WHERE uf.id IS NULL or uf.value != p.partisanship
ORDER BY RANDOM()
