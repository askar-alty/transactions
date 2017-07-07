
## запуск map reduce задачи
## для группировки пользователь с агрегированием mcc кодов
hadoop jar GroupByClientsAggregateByMCC/target/GroupByClientsAggregateByMCC-1.0-SNAPSHOT.jar AverageByMCC /user/hive/warehouse/trxs/transactions_sample.csv /user/sa/output


## сохраняю в локальную файловую систему 
hadoop fs -get /user/sa/output/part-r-00000 clients_mcc.csv