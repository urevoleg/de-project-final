Кирилл, привет!

Спасибо за проверку. Отдельные проекции упустил из виду, добавил к созданию таблицы `traqnsactions` 
создание проекции по дате:

```sql
CREATE PROJECTION IF NOT EXISTS transactions_by_dates as
SELECT *
FROM UREVOLEGYANDEXRU__STAGING.transactions
ORDER BY transaction_dt
SEGMENTED BY hash(transaction_dt::date) all nodes;
```
