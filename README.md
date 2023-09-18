# Решение тестового задания Data Engineer Meowpunk

Сильно красоту наводить не стал, т.к. ограничился одним вечером на оба решения.

## [Решение 1 без библиотек](solution.py)

**Плюсы**

- Работает быстрее
- Потребляет мало памяти

**Минусы**

- Много кода

### Лог

```
INFO:root:Start loading with config {'client_path': 'data/client.csv', 'server_path': 'data/server.csv', 'cheaters_path': 'data/cheaters.db', 'result_path': 'data/result.db', 'day': datetime.date(2021, 5, 18), 'start_ts': 1621296000, 'end_ts': 1621382399, 'batch_size': 100}
INFO:root:Read client file
INFO:root:Total client rows 514
INFO:root:Read server file
INFO:root:Total server rows 468
INFO:root:Read cheaters db
INFO:root:Total cheaters rows 16577
INFO:root:Start writing results
INFO:root:Writing batch #1
INFO:root:Writing batch #2
INFO:root:Total result rows 180
INFO:root:Loading completed
INFO:root:Time taken = 1.6873300075531006 sec
```

### График потребления памяти
![memory_used.png](png%2Fmemory_used.png)

## [Решение 2 с помощью pandas](solution_pandas.py)

**Плюсы**

- Меньше кода

**Минусы**

- Работает быстрее
- Потребляет мало памяти

### Лог

```
INFO:root:Start loading with config {'client_path': 'data/client.csv', 'server_path': 'data/server.csv', 'cheaters_path': 'data/cheaters.db', 'result_path': 'data/result.db', 'day': datetime.date(2021, 5, 18), 'start_ts': 1621296000, 'end_ts': 1621382399, 'batch_size': 100}
INFO:root:Read client file
INFO:root:Total client rows 514
INFO:root:Read server file
INFO:root:Total server rows 468
INFO:root:Read cheaters db
INFO:root:Total cheaters rows 16577
INFO:root:Result rows 180
INFO:root:Loading completed
INFO:root:Time taken = 2.865921974182129 sec
```

### График потребления памяти
![memory_used_pandas.png](png%2Fmemory_used_pandas.png)
