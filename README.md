# BossKG
Main job:
1. 数据清洗，清洗目前KG库里的各类数据(空值、简体繁体转换、特殊符号、重复等等)
2. 数据准入框架，保证入库数据格式的准确性
3. Kanzhun、CRM、Boss数据入库(getKafka和putKafka)
4. IDMapping映射，将CrmID, BossID, KzID匹配并入库(入库的唯一性策略，ReduceByKey)
