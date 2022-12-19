MERGE INTO UREVOLEGYANDEXRU__DWH.global_metrics tgt
USING
	(WITH last_status_transactions as(-- last status
										SELECT *,
											   ROW_NUMBER () OVER(PARTITION BY t.operation_id ORDER BY transaction_dt desc) AS rn
										FROM UREVOLEGYANDEXRU__STAGING.transactions t
										WHERE t.account_number_from >= 0)
		SELECT t.transaction_dt::date AS date_update,
			   t.currency_code AS currency_from,
			   sum(t.amount * c.currency_with_div) AS amount_total,
			   count(*) AS cnt_transactions,
			   round(1.0 * count(*) / count(DISTINCT t.operation_id), 2) AS avg_transactions_per_account,
			   count(DISTINCT t.operation_id) AS cnt_accounts_make_transactions
		FROM last_status_transactions t
		JOIN UREVOLEGYANDEXRU__STAGING.currencies c
		ON t.transaction_dt::date = c.date_update::date
		AND t.currency_code = c.currency_code_with
		WHERE t.rn = 1 AND t.status IN ('done', 'chargeback')
		AND c.currency_code = 420
		GROUP  BY 1, 2
		ORDER BY 1) src
ON tgt.date_update = src.date_update AND tgt.currency_from = src.currency_from
WHEN MATCHED AND (tgt.amount_total <> src.amount_total OR tgt.cnt_transactions <> src.cnt_transactions OR tgt.avg_transactions_per_account <> src.avg_transactions_per_account OR tgt.cnt_accounts_make_transactions <> src.cnt_accounts_make_transactions)
	THEN UPDATE SET amount_total = src.amount_total ,cnt_transactions = src.cnt_transactions ,avg_transactions_per_account = src.avg_transactions_per_account ,cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
WHEN NOT MATCHED
    THEN INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
    VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);
