CREATE TABLE IF NOT EXISTS UREVOLEGYANDEXRU__STAGING.currencies (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);