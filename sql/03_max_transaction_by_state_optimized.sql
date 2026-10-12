-- Merge обязателен: состояния могут находиться в нескольких несмерженных parts.
SELECT
    us_state,
    tupleElement(winner, 1) AS category,
    tupleElement(winner, 2) AS max_amount
FROM
(
    SELECT us_state, argMaxMerge(winner) AS winner
    FROM homework.state_max
    GROUP BY us_state
)
ORDER BY us_state
FORMAT CSVWithNames;

