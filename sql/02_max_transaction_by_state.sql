-- Категория крупнейшей отдельной транзакции, а не сумма по категории.
-- При равной сумме выбираем максимум (source_file, row_number).
SELECT
    us_state,
    tupleElement(winner, 1) AS category,
    tupleElement(winner, 2) AS max_amount
FROM
(
    SELECT
        us_state,
        argMax(tuple(cat_id, amount), tuple(amount, source_file, row_number)) AS winner
    FROM homework.transactions_baseline
    GROUP BY us_state
)
ORDER BY us_state
FORMAT CSVWithNames;

