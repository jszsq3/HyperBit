# HyperBit-v2.0(2022-12-05)
一个可以存储并查询时序RDF图的图数据库系统。
## 开始前
开始使用前，首先在文件夹下
``` shell 
make
```
可执行文件放在bin/lrelease/下
## 建库
在第一次创建数据库时可以使用带有时间标签的四元组数据， 形如 \<s\> \<p\> \<o\> \<t\> . 
``` shell
bin/lrelease/buildTripleBitFromN3  <数据地址和名称> <数据库存放地址>
```
例如
``` shell
bin/lrelease/buildTripleBitFromN3  t_1w1.nt database/
```
## 插入
插入只支持插入普通三元组， 形如 \<s\> \<p\> \<o\> ， 时间则为插入系统的时间， 由系统自动生成。

``` shell
bin/lrelease/buildTripleBitFromN3 <数据库存放地址> <插入数据地址和名称>
```

例如
``` shell
bin/lrelease/triplebitInsert database/ 1w1.nt
```
# 查询
支持SPARQL时序查询，样例在文件夹 sparql/ 下

``` shell
bin/lrelease/triplebitQuery <数据库存放地址> <查询语句所在文件夹>
```

例如
``` shell
bin/lrelease/triplebitInsert database/ sparql/
```

首先会加载数据库 loading...
加载完成后，出现“ \>\> ” 提示输入查询语句文件。

# 2023年2月15日更新
 加入统计信息模块(未完成)