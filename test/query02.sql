\set company_id random(1, 2000)
BEGIN;
    SELECT
        co.id as company_id,
        co.name as company_name,
        SUM(ca.monthly_budget) as monthly_budget
    FROM
        public.companies co
    INNER JOIN
        public.campaigns ca ON co.id = ca.company_id
    WHERE
        co.id = :company_id
    GROUP BY 1;
END;


"query02.sql" 14L, 326C