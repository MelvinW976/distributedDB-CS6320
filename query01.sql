\set company_id1 random(1, 2000)
\set company_id2 random(1, 2000)
\set company_id3 random(1, 2000)
BEGIN;
    SELECT
        ad.company_id,
        ad.id,
        count(*)
    FROM
        public.ads ad 
    INNER JOIN
        public.campaigns ca ON ad.campaign_id = ca.id AND ad.company_id = ca.company_id
    WHERE
        ad.company_id IN (
            :company_id1,
            :company_id2,
            :company_id3
        )

"query01.sql" 21L, 462C