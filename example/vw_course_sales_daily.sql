CREATE OR REPLACE VIEW vw_course_sales_daily AS
SELECT
  t.course_id,
  DATE(t.captured_at) AS captured_date,
  t.price,
  t.sold_num
FROM (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY s.course_id, DATE(s.captured_at)
      ORDER BY s.sold_num DESC, s.captured_at DESC, s.id DESC
    ) AS rn
  FROM hahow_course_sales s
) AS t
WHERE t.rn = 1;