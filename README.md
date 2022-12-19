Переводите ваши денежки:

![](https://srochnyj-kredit.ru/images_article-2/finans-kredit-02.jpg)

# Fintech

**ASIS**
```markdown
Команда аналитиков попросила собрать данные по транзакционной активности пользователей и настроить обновление таблицы с курсом валют.
**Цель** — понять, как выглядит динамика оборота всей компании и что приводит к его изменениям.
```

**Sources**

_1. Backend  мобильного приложения, PostgreSQL. Таблицы:_

Таблицы:
1. `public.transactions` - таблица транзакций,  содержит в себе информацию о движении денежных средств между клиентами в разных валютах:
   2. `operation_id` - id транзакции
   3. `account_number_from` - внутренний бухгалтерский номер счёта транзакции ОТ КОГО
   4. `account_number_to` - внутренний бухгалтерский номер счёта транзакции К КОМУ
   5. `currency_code` - трёхзначный код валюты страны, из которой идёт транзакция
   6. `country` -  страна-источник транзакции
   7. `status` - статус проведения транзакции: `queued` («транзакция в очереди на обработку сервисом»), `in_progress` («транзакция в обработке»), `blocked` («транзакция заблокирована сервисом»), `done` («транзакция выполнена успешно»), `chargeback` («пользователь осуществил возврат по транзакции»)
   8. `transaction_type` - тип транзакции во внутреннем учёте: `authorisation` («авторизационная транзакция, подтверждающая наличие счёта пользователя»), `sbp_incoming` («входящий перевод по системе быстрых платежей»), `sbp_outgoing` («исходящий перевод по системе быстрых платежей»), `transfer_incoming` («входящий перевод по счёту»)
   9. `transfer_outgoing` - `c2b_partner_incoming` («перевод от юридического лица»), `c2b_partner_outgoing` («перевод юридическому лицу»).
   10. `amount` - целочисленная сумма транзакции в минимальной единице валюты страны (копейка, цент, куруш)
   11. `transaction_dt` - дата и время исполнения транзакции до миллисекунд
12. `public.сurrencies` - справочник, который содержит в себе информацию об обновлениях курсов валют и взаимоотношениях валютных пар друг с другом:
    13. `date_update` - дата обновления курса валют
    14. `currency_code` - трёхзначный код валюты транзакции
    15. `currency_code_with` - отношение другой валюты к валюте трёхзначного кода
    16. `currency_code_div` - значение отношения единицы одной валюты к единице валюты транзакции


**Архитектура решения**

![fintech-arch.png](src%2Fimg%2Ffintech-arch.png)

ps: из-за особенностей VERTICA загружать данные в таблицы эффективнее при помощи команды `COPY`, поэтому будет реализована
выгрузка из PostgreSQL в локальные файлы с последующей загрузкой в VERTICA.

---------------------------------------------------

# План решения

![plan.png](src%2Fimg%2Fplan.png)

1. Создать необходимые слои в DWH:
   2. `STAGING` - логика загрузки, **asis** - данные из источника в исходном виде загружаются в таблицы staging слоя
   3. `DWH` - datamart layer. Здесь будет расположена требуемая заказчиком витрина `global_metrics`
4. Пайплан загрузки из источников:
   5. DAG Airflow c ежедневным запуском и инкрементальной загрузкой данных за предыдущий день
6. Пайплайн инкрементального обновления витрины

Витрина `global_metrics` должна иметь следующие поля:
- `date_update` - дата расчета
- `currency_from` - код валюты транзакции
- `amount_total` - общая сумма транзакций по валюте в долларах **(?за доллар принимаем код USA?)**
- `cnt_transactions` - общий объём транзакций по валюте;
- `avg_transactions_per_account` - средний объём транзакций с аккаунта
- `cnt_accounts_make_transactions` - количество уникальных аккаунтов с совершёнными транзакциями по валюте

**ASIS**: витрина должна отвечать на вопросы:
- какая ежедневная динамика сумм переводов в разных валютах;
- какое среднее количество транзакций на пользователя;
- какое количество уникальных пользователей совершают транзакции в валютах;
- какой общий оборот компании в единой валюте.

![datamart.png](src%2Fimg%2Fdatamart.png)

--------------------------------------------------

# Миграции

![migrations.jpg](src%2Fimg%2Fmigrations.jpg)

1. `transactions`
```sql
CREATE TABLE  UREVOLEGYANDEXRU__STAGING.transactions (
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
```

2. `currencies`
```sql
CREATE TABLE UREVOLEGYANDEXRU__STAGING.currencies (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);
```

3. `srv_wf_settings` - сервисная таблица для фиксации курсора при инкрементальной загрузке
```sql
CREATE TABLE IF NOT EXISTS UREVOLEGYANDEXRU__STAGING.srv_wf_settings (
	id AUTO_INCREMENT,
	source varchar NOT NULL,
	settings varchar NOT NULL,
	loaded_at timestamp NOT NULL
)
order by loaded_at
SEGMENTED BY hash(loaded_at::date) all nodes
PARTITION BY loaded_at::date
GROUP BY calendar_hierarchy_day(loaded_at::date, 3, 2);
```

# Пайплан загрузки из источников

В соответствии с архитектурой решения необходимо реализовать:
- чтение последнего загруженного ключа `last_key` из таблицы `srv_wf_settings`
- выгрузка новых данных из источника в файл, где дата > `last_key`
- загрузка данных в хранилище из файла

