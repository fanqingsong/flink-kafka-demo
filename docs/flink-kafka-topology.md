# 看图理解：Flink 怎么读 Kafka

把 Kafka 想成邮局，把 Flink 想成订了某个信箱的会计。会计只读信、算账，不改邮局，也不改寄信的人。

先读这篇会更容易：[统计销量，为什么不直接用数据库？](database-vs-flink.md)

## 1. 谁干什么

```mermaid
flowchart LR
  shop["收银系统<br/>寄信的人"] --> box["Kafka topic<br/>demo.purchases<br/>一个信箱"]
  box --> accountant["Flink 作业<br/>订了这个信箱的会计"]
  accountant --> result["另一个信箱<br/>demo.running.totals<br/>算出的累计账"]
```

信箱里的原信还在。会计另外开一个信箱，把算好的结果投进去。

本仓库有两个会计：

```mermaid
flowchart TB
  purchases["信箱 demo.purchases<br/>一笔笔购买"]
  products["信箱 demo.products<br/>商品资料"]

  purchases --> totals["RunningTotals<br/>按商品把销量累加"]
  purchases --> join["JoinStreams<br/>购买记录配上商品资料"]
  products --> join

  totals --> totalsBox["信箱 demo.running.totals"]
  join --> joinBox["信箱 demo.purchases.enriched"]
```

订哪个信箱，写在 `src/main/resources/config.properties` 里。换一套 Kafka，改地址和信箱名就行。

## 2. 两套集群各管各的

Kafka 集群负责把信存好。Flink 集群负责算账。它们可以装在不同的机器上。

```mermaid
flowchart LR
  subgraph kafka["Kafka 集群 · 存信"]
    b1["机器 1<br/>Broker"]
    b2["机器 2<br/>Broker"]
  end
  subgraph flink["Flink 集群 · 算账"]
    t1["机器 3<br/>TaskManager"]
    t2["机器 4<br/>TaskManager"]
  end
  kafka -->|"把信拉过去"| flink
```

一台 Kafka 机器不必配一台 Flink 机器。邮局人少、会计人多，或反过来，都可以。放在同一个机房就够了，免得信件跨城市跑。

## 3. 信放在一起，也不会少跑腿

就算 Kafka 和 Flink 装在同一批机器上，读信的人也不一定就站在信箱旁边。

下面两台机器，每台都有一个邮局窗口（Broker）和一个会计（TaskManager）。P0、P1 是同一个信箱里的两个格子。

```mermaid
flowchart LR
  subgraph ma["机器 A"]
    ba["邮局窗口 A<br/>格子 P0 的信在这"]
    ta["会计 A<br/>却在读格子 P1"]
  end
  subgraph mb["机器 B"]
    bb["邮局窗口 B<br/>格子 P1 的信在这"]
    tb["会计 B<br/>却在读格子 P0"]
  end

  bb -->|"跨过网络去取 P1"| ta
  ba -->|"跨过网络去取 P0"| tb
```

谁读哪个格子，是临时分的，不看信实际放在哪台机器。所以大多数信还是要从一台机器搬到另一台。碰巧分到同一台时，才走机器内部，省掉网线那一跳。

## 4. 算账时还会再搬一次

`RunningTotals` 要按商品编号把账加在一起。同一个商品的信，必须送到同一个会计手上。这一步叫 `keyBy`。

```mermaid
flowchart LR
  subgraph ma["机器 A · 会计 A"]
    ta["刚读到的购买<br/>奶茶、咖啡、奶茶"]
  end
  subgraph mb["机器 B · 会计 B"]
    tb["负责奶茶的总账"]
  end
  ta -->|"奶茶留下给自己<br/>咖啡送到对面"| tb
```

这次搬家发生在两个会计之间，和邮局在不在旁边无关。

一条购买记录的完整路线：

```mermaid
flowchart LR
  kafka["Kafka 格子"] -->|"第 1 跳：把信拉过来"| source["会计读信"]
  source -->|"第 2 跳：keyBy<br/>相同商品送到同一个人"| reduce["把这个商品的账累加"]
  reduce --> out["写进结果信箱"]
```

第 1 跳有时碰巧在本机。第 2 跳只要两个会计不在同一台机器，就一定过网络。

## 5. 想少搬，就少寄信

现在每买一杯，就寄一封信过去做累加。可以让每个会计先在自己桌上把同一商品加一加，过一会儿只寄一张小计。

```mermaid
flowchart TB
  subgraph now["现在"]
    a1["奶茶 1 杯"] --> net1["网络"]
    a2["奶茶 1 杯"] --> net1
    a3["奶茶 1 杯"] --> net1
    net1 --> sum1["对面收 3 封信再累加"]
  end
  subgraph better["改成先本地小计"]
    b1["奶茶 1 杯"] --> local["自己桌上加好：3 杯"]
    b2["奶茶 1 杯"] --> local
    b3["奶茶 1 杯"] --> local
    local --> net2["网络只送 1 张小计"]
    net2 --> sum2["对面把小计合并"]
  end
```

购买越多、商品种类越少，省下的网络越多。本仓库现在用的是左边这种，每笔都送。
