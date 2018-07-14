-------------------------------------------------------------------------------
-- REMOVE HOME URL ARTICLE
-------------------------------------------------------------------------------
-- TESTS
-------------------------------------------------------------------------------
-- SELECT id, group_id
-- FROM article
-- WHERE canonical_url NOT SIMILAR TO 'https?://[^/]+/_%';
-- 
-- SELECT COUNT(id) 
-- FROM article
-- WHERE canonical_url NOT SIMILAR TO 'https?://[^/]+/_%';
-- 
-- 
-- SELECT u.id, u.article_id
-- FROM url as u
-- JOIN article AS a ON a.id=u.article_id
-- WHERE a.canonical_url NOT SIMILAR TO 'https?://[^/]+/_%'
-- ORDER BY u.id ASC
-- LIMIT 5
-- ;
-- 
-- SELECT COUNT(u.id) 
-- FROM url as u
-- JOIN article AS a ON a.id=u.article_id
-- WHERE a.canonical_url NOT SIMILAR TO 'https?://[^/]+/_%'
-- ;
-------------------------------------------------------------------------------

UPDATE url AS u
SET article_id=NULL
FROM article AS a
WHERE a.id=u.article_id
  AND a.canonical_url NOT SIMILAR TO 'https?://[^/]+/_%'
;

DELETE FROM article
WHERE canonical_url NOT SIMILAR TO 'https?://[^/]+/_%';


-------------------------------------------------------------------------------
-- SET HTML=NULL FOR UNCENESSARY RECORDS TO SAVE SPACED FOR ARTICLE UPDATE
-- site_id IS NULL AND html IS NOT NULL
-------------------------------------------------------------------------------
-- TESTS
-------------------------------------------------------------------------------
-- SELECT COUNT(*)
-- FROM url
-- WHERE site_id IS NULL
--   AND html IS NOT NULL
-- ;
-------------------------------------------------------------------------------

UPDATE url
SET html=NULL
WHERE site_id IS NULL
  AND html IS NOT NULL
;

-------------------------------------------------------------------------------
-- RESET status_code of article with no group_id
-------------------------------------------------------------------------------
-- TESTS
-------------------------------------------------------------------------------
-- SELECT id, group_id, status_code, canonical_url
-- FROM article
-- WHERE site_id IS NOT NULL
--   AND status_code=80
--   AND group_id IS NULL
-- ;
-------------------------------------------------------------------------------

UPDATE article SET status_code=0
WHERE site_id IS NOT NULL
  AND status_code=80
  AND group_id IS NULL
;

-------------------------------------------------------------------------------
-- UPDATE article with HTML
-------------------------------------------------------------------------------
-- TESTS
-------------------------------------------------------------------------------
-- SELECT MAX(id)
-- FROM article WHERE html IS NULL;
--    max
-- ---------
--  2106399
-- (1 row)
-- 
-- SELECT MIN(id)
-- FROM article WHERE html IS NOT NULL;
--    min
-- ---------
--  2106400
-- (1 row)
-- 
-- SELECT Id, created_at, md5(html) FROM Url WHERE Article_id=2106432;
--     id    |         created_at         |               md5                
-- ----------+----------------------------+----------------------------------
--  14300850 | 2018-06-24 15:47:09.788937 | 
--  14298696 | 2018-06-24 13:59:06.291231 | 
--  14297446 | 2018-06-24 12:35:25.984298 | 
--  14295872 | 2018-06-24 09:00:30.472146 | 10008534b768d3ebfdfc2ccda9c81c5e
-- (4 rows)
-- 
-- 
-- SELECT Md5(html) FROM Article WHERE id=2106432;
--                md5                
-- ----------------------------------
--  10008534b768d3ebfdfc2ccda9c81c5e
-- (1 row)
-- 
-- WITH pri_url AS (
--   SELECT DISTINCT ON (u.canonical) u.article_id, u.html
--   FROM url AS u
--   JOIN article AS a ON a.id=u.article_id
--   WHERE a.id<=2106477 AND a.id>=2106400
--   ORDER BY u.canonical, u.created_at ASC
-- )
-- SELECT COUNT(pri_url.article_id)
-- FROM pri_url
-- ;
-- 
-- WITH pri_url AS (
--   SELECT DISTINCT ON (u.canonical) u.article_id, u.html
--   FROM url AS u
--   JOIN article AS a ON a.id=u.article_id
--   WHERE a.id<=2106477 AND a.id>=2106400
--   ORDER BY u.canonical, u.created_at ASC
-- )
-- UPDATE article
-- SET html=pri_url.html
-- FROM pri_url
-- WHERE pri_url.article_id=article.id
-- ;
-- 
-- 
-- WITH pri_url AS (
--   SELECT DISTINCT ON (u.canonical) u.article_id, u.html
--   FROM url AS u
--   JOIN article AS a ON a.id=u.article_id
--   WHERE a.id<2106400
--   ORDER BY u.canonical, u.created_at ASC
-- )
-- SELECT COUNT(*) FROM pri_url;
--   count  
-- ---------
--  1301354
-- (1 row)
-------------------------------------------------------------------------------
WITH pri_url AS (
  SELECT DISTINCT ON (u.canonical) u.article_id, u.html
  FROM url AS u
  JOIN article AS a ON a.id=u.article_id
  WHERE a.id<2106400
  ORDER BY u.canonical, u.created_at ASC
)
UPDATE article
SET html=pri_url.html
FROM pri_url
WHERE pri_url.article_id=article.id
;
