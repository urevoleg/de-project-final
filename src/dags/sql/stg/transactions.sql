CREATE TABLE IF NOT EXISTS UREVOLEGYANDEXRU__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL
)
order by transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt::date) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);


CREATE PROJECTION IF NOT EXISTS transactions_by_dates as
SELECT *
FROM UREVOLEGYANDEXRU__STAGING.transactions
ORDER BY transaction_dt
SEGMENTED BY hash(transaction_dt::date) all nodes;