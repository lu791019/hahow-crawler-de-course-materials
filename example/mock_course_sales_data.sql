INSERT INTO hahow_course_sales (course_id, price, sold_num, captured_at, uploaded_at)
SELECT
  z.course_id,
  z.price_today                               AS price,
  GREATEST(z.sold_today - z.dec_total, 0)     AS sold_num,
  z.cap_now - INTERVAL z.step DAY             AS captured_at,
  NOW()                                       AS uploaded_at
FROM (
  SELECT
    t.course_id, t.price_today, t.sold_today, t.cap_now, t.step,
    SUM(t.rnd) OVER (
      PARTITION BY t.course_id
      ORDER BY t.step
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dec_total
  FROM (
    SELECT
      l.course_id,
      l.price_today,
      l.sold_today,
      l.cap_now,
      l.pub_date,             -- ← 帶出 publish_time（日期）
      n.step,
      FLOOR(
        RAND() * (
          @lo + (@hi - @lo) * (n.step / NULLIF(l.max_step, 1))
        )
      ) AS rnd
    FROM (
      SELECT
        s.course_id,
        s.price       AS price_today,
        s.sold_num    AS sold_today,
        s.captured_at AS cap_now,
        DATE(c.publish_time) AS pub_date,   -- ← 這裡取出 publish_time 供外層使用
        -- 從今天最新快照回推到 max(2025-01-01, publish_time) 的最大天數
        DATEDIFF(
          DATE(s.captured_at),
          GREATEST(DATE('2025-01-01'), COALESCE(DATE(c.publish_time), DATE('1970-01-01')))
        ) AS max_step
      FROM hahow_course_sales s
      JOIN (
        SELECT course_id, MAX(captured_at) AS max_cap
        FROM hahow_course_sales
        WHERE DATE(captured_at) = CURDATE()
        GROUP BY course_id
      ) m ON m.course_id = s.course_id AND m.max_cap = s.captured_at
      JOIN hahow_course c ON c.id = s.course_id
    ) AS l
    JOIN (
      -- 1..2000 的步數
      SELECT (a.n + b.n*10 + c.n*100) + 1 AS step
      FROM (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
      CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
      CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
    ) AS n
    CROSS JOIN (SELECT @lo := 1, @hi := 10) AS cfg
    WHERE n.step <= l.max_step
      AND DATE(l.cap_now - INTERVAL n.step DAY) >= DATE('2025-01-01')         -- 不早於 2025-01-01
      AND (l.pub_date IS NULL OR DATE(l.cap_now - INTERVAL n.step DAY) >= l.pub_date)  -- 不早於 publish_time
  ) AS t
) AS z
LEFT JOIN hahow_course_sales exist
  ON exist.course_id = z.course_id
 AND DATE(exist.captured_at) = DATE(z.cap_now - INTERVAL z.step DAY)
WHERE z.dec_total < z.sold_today   -- 歸零即停
  AND exist.id IS NULL;
