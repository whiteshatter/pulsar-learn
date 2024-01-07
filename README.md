# 《Apache Pulsar原理解析与应用实践》

# 01 Pulsar概述
## 1.1 Pulsar是什么
Pulsar是一个分布式发布、订阅（pub-sub）消息的平台，具有非常灵活的消息传递模型以及跨语言的客户端API。Pulsar也是一个集消息传递、消息存储、轻量化函数式计算于一体的流数据平台。Pulsar采用了计算与存储分离的架构，支持云原生、多租户、持久化存储、多机房跨区域数据复制等，具有高一致性、高吞吐、低延时及高可扩展性等流数据存储系统特性。

## 1.2 Pulsar的优势
在诸多优秀的开源消息队列，如RabbitMQ、RocketMQ和Kafka的基础上，Pulsar实现了很多上一代消息系统或者上一代流数据系统没有实现的功能和特性，比如云原生、多租户、存储与计算分离、分层存储等。
### 1.2.1 Pulsar不只是消息队列
1. Pulsar是一个分布式消息平台，可以同时处理流式数据和异构系统数据对接这两类问题。这是因为Pulsar具有非常灵活的消息传递模型。
2. 为了实现更加丰富的消费模式，Pulsar提出了订阅的概念。Pulsar提供了多种订阅模式—独占模式（Exclusive）、故障切换模式（Failover）、共享模式（Shared）、键共享模式（Key_Shared）
3. Pulsar是一个集消息传递、消息存储、轻量化函数式计算于一体的流数据平台。
4. Pulsar是一个分布式可扩展的流式存储系统，并在数据存储的基础上构建了消息队列和流服务的统一模型。

### 1.2.2 存储与计算分离
**1.需要分离原因**

在Kafka中，每个分区的管理与存储职能都依赖其中一个服务端节点（承担分区Leader角色的Broker节点）。该节点在处理数据写入请求的同时，会将数据写到本机的存储路径下，并负责向其他副本写入数据。在数据的读取过程中，该节点负责读取磁盘数据并发送回客户端。在Kafka的架构中，存储与计算功能都由一个Kafka Broker节点负责。

无论计算资源和存储资源谁先达到瓶颈，都需要增加机器，所以可能会浪费很多机器资源。当对存储资源进行扩容时，在计算和存储混合的模式下可能需要迁移大量数据，进而大大增加扩容成本。

**2.Pulsar存储计算分离原理分析**

Pulsar是一个存储与计算分离的消息队列，其中提供计算服务的角色被称为Broker，提供存储服务的角色被称为Bookie

Broker提供的是无状态的计算服务，在计算资源不足时可独立扩容。Bookie提供的是有状态的存储服务，Pulsar中的数据会以数据块的形式分配到不同的Bookie节点。当存储资源不够时，可通过增加Bookie节点进行扩容。Pulsar会感知Bookie集群的变化，并在合适的时机使用新增加的Bookie节点进行存储，这就避免了人为迁移数据的操作。

### 1.2.3 云原生架构
Pulsar是一个云原生应用，拥有诸多云原生应用的特性，如无状态计算层、计算与存储分离，可以很好地利用云的弹性（伸缩能力），从而具有足够高的扩容性和容错性。Pulsar采用的存储与计算分离架构在云原生环境中有着更大的价值。Pulsar实例中的存储节点可以由一组Bookie容器负责，计算节点由一组Broker容器负责。存储与计算节点可以完全独立扩缩容，通过Kubernetes这样的容器编排工具，业务方可以快速构建可弹性扩缩容的云原生消息队列。

### 1.2.4 存储特性
Pulsar利用BookKeeper实现了分块存储的能力，在BookKeeper中，Ledger代表一个独立日志块或一段数据流，是持久化存储的单元。记录会被有序地写入Ledger中。数据一经写入就不允许进行修改了。Pulsar能够将每个主题映射为多个独立的数据段，每个数据段对应一个Ledger。

### 1.2.5 消息传输协议
目前Pulsar已经支持Kafka、RocketMQ、AMQP和MQTT等多种协议。

### 1.2.6 消费方式
RocketMQ与Kafka都基于Pull模式进行数据读取。Pull模式的优势在于可以控制数据的消费速度和消费数量，保证消费者不会达到饱和状态。但是在没有数据时，会出现多次空轮询，浪费计算资源。

Pulsar中的消费者在读取数据时采用以Push模式为主、Pull模式为辅的同步模式。Pulsar中的客户端有一个缓冲队列。客户端会向服务端发送流量配额请求，服务端会主动向客户端推送配额允许范围内的数据。消费者连接建立后，服务端通过配额向消费者推送数据。与此同时，消费者每消费一条数据都会增加客户端流量配额计数，在配额计数达到队列的一半时，客户端会再次发送流量配额请求，请求服务端推送数据。

### 1.2.7 性能与可靠性
Pulsar通过BookKeeper实现了数据的高可靠。在BookKeeper中Ledger是基本的持久化存储单元。Pulsar的每个主题的数据都会在逻辑上映射为多个Ledger。每个Ledger在服务端会存储多个副本。

在Broker端每个主题都是逻辑主题，这使其可以轻松支持海量主题。为了获取更好的性能，BookKeeper客户端在写入多副本时，也是由客户端完成多副本写入操作的，而不是采用服务端复制的方式

### 1.2.8 语义支持与一致性级别
Pulsar可以通过幂等生产者在单个分区上写入数据，并保证其可靠性。通过客户端的自增序列ID、重试机制与服务端的去重机制，幂等生产者可以保证发送到单个分区的每条消息只会被持久化一次，且不会丢失数据。

### 1.2.9 扩展能力
Pulsar的服务端的Broker负责接入、计算、分发消息等职能，Bookie负责消息存储，两者均可以按需动态地进行扩缩容处理。服务端会周期性地获取各个Broker节点的负载情况，并根据负载情况进行负载均衡，即每次扩容后都可以自动进行负载均衡。

### 1.2.10 消息模式
Pulsar采用的是发布订阅模式

### 1.2.11 多租户
多租户是一种软件架构技术，主要用来实现多用户的环境下共用相同的系统或程序组件，并确保各用户的数据具有一定的隔离性。

Pulsar租户可以分布在多个集群中，并且每个租户都可以应用自己的身份验证和授权方案。命名空间是租户内的独立管理单元。在命名空间上设置的配置策略适用于在该命名空间中创建的所有主题。

### 1.2.12 延迟队列
支持秒级的延迟消息，所有延迟投递的消息都会被内部组件跟踪，消费组在消费消息时，会先去延迟消息追踪器中检查，以明确是否有到期需要投递的消息。如果有到期的消息，则根据追踪器找到对应的消息进行消费；如果没有到期的消息，则直接消费非延迟的正常消息。延迟时间长的消息会被存储在磁盘中，当快到延迟时间时才被加载到内存里。

### 1.2.13 重试和死信队列
在Pulsar中某些消息可能会被多次重新传递，甚至可能永远都在重试中。通过使用死信队列，可让消息具有最大重新传递次数。当实际传递次数超过最大重新传递次数时，对应的消息会被发送到死信主题并自动确认。

# 02 Pulsar的基本概念和架构详解
## 2.1 多租户与命名空间
Pulsar的多租户系统可在多租户环境下使用同一套程序，且保证租户间资源与数据隔离。命名空间是租户内的独立管理单元。用户可以通过设置不同级别的策略来管理租户和命名空间，可以独自管理存储配额、消息过期时间（TTL）和隔离策略等配置。

## 2.2 主题
在Pulsar中，消息存储与管理的基本单位是主题，而在主题内消息的基本管理单位是数据段，所以可以认为数据段是Pulsar中最小的信息管理单位。

### 2.2.1 分区主题和非分区主题
在非分区主题中，主题被创建后，不能修改分区，整个主题由一个Borker服务节点独立提供服务，服务包括订阅管理、消费者管理、生产者管理等。而分区主题在逻辑上可被划分为多个非分区主题。假设persistent://public/default/topic被划分为3个非分区主题—persistent://public/default/topic-parition-0、persistent://public/default/topic-parition-1、persistent://public/default/topic-parition-2，那么persistent://public/default/topic就是一个分区主题。通过主题名可以指定主题的生产或者消费方式，此时Pulsar会自动根据路由规则访问带有“-partition-*”的逻辑分区主题。

```shell
#创建非分区主题
cd ../pulsar2.7/
bin/pulsar-admin topics create persistent://public/default/topic
bin/pulsar-admin topics list public/default
"persistent://public/default/topic"
"persistent://public/default/my-topic"

#创建分区主题
bin/pulsar-admin topics create-partitioned-topic persistent://public/default/partition-topic -p 3
bin/pulsar-admin topics list public/default
"persistent://public/default/partition-topic-partition-0"
"persistent://public/default/partition-topic-partition-1"
"persistent://public/default/partition-topic-partition-2"
"persistent://public/default/my-topic"

```
### 2.2.2 持久化主题和非持久化主题
Pulsar默认创建持久化主题，在非持久化主题中，服务端收到消息后会直接转发给当前的所有消费者。非持久化主题的地址以non-persistent开头，例如non-persistent://tenant/namespace/topic

## 2.3 生产者
![生产者与服务端关系图](src/main/resources/static/生产者与服务端关系图.jpg "生产者与服务端关系图")
1. 一对一
- 由服务端处理该生产者的所有消息发送请求
2. 一对多
- 轮询路由模式：该模式可保证吞吐量优先，但是单个分区的消息会被打散分发到各个服务端节点。
- 单分区模式：该模式会将当前生产者的所有消息分发到某个分区。
3. 多对多
- 共享模式：共享模式下多个生产者都可以发送消息到服务端，服务端按照接收顺序依次处理各条消息。
- 独占模式：独占模式下仅有一个生产者可以连接至服务端。如果新生产者在建立之前已经有其他生产者成功连接至服务端，那么新的生产者的建立会失败并抛出异常。
- 独占等待模式：独占等待模式仅有一个生产者可以发送消息至服务端。在新建生产者时，如果对应服务端已经连接了其他生产者，则生产者新建进程将被挂起，直到生产者获得独占访问权限。

## 2.4 消费者与订阅
![消费者与订阅](src/main/resources/static/Pulsar中的消费者与订阅.jpg)
## 2.4.1 订阅
默认情况下Pulsar提供的是持久化订阅，使用同一个订阅的消费者会在服务端存储消费的位置，并在重启后在上次消费的位置继续消费消息。在客户端，用户可以选择非持久化订阅模式。消费者在使用非持久化订阅模式时，若是退出消费，则当前订阅会被服务端释放，且不会存储本次消费的位置。

### 2.4.1.1 独占模式
![独占模式](src/main/resources/static/独占模式.jpg)

每个订阅只允许一个消费者接入。若订阅的主题是多分区主题，消费者也会独占多个分区。独占模式是Pulsar默认的订阅模式。

### 2.4.1.2 故障转移模式
![故障转移模式](src/main/resources/static/故障转移模式.jpg)

Pulsar会为非分区主题或分区主题的每个分区选择一个主消费者并接收其消息。当主消费者断开连接时，所有消息都会传递给下一个消费者。对于分区主题，服务端将根据优先级和消费者的名称在字典中的顺序对消费者进行排序。然后，服务端会尝试将主题均匀分配给具有最高优先级的消费者。对于非分区主题，服务端将按照消费者订阅非分区主题的顺序选择消费者。

### 2.4.1.3 共享模式
![共享模式](src/main/resources/static/共享模式.jpg)

消息以循环分配的方式在消费者之间传递，并且任何给定的消息都只传递给一个消费者。当消费者断开连接时，所有发送给它但未被确认的消息将被重新安排发送给剩余的消费者。在这种模式下，由于消息发送具有随机性，所以多个消费者之间不能保证消息有序。

### 2.4.1.3 键共享模式
![键共享模式](src/main/resources/static/键共享模式.jpg)

消息可以在多个消费者中传递，具有相同键的消息仅会被传递给一个消费者。在使用键共享模式时，必须为消息指定一个键。

### 2.4.1.4 传统发布订阅模式
![发布-订阅模式](src/main/resources/static/发布-订阅模式.jpg)

应为每个消费者指定唯一的订阅名称，并使用独占模式。在这种配置下，每个消费者都会消费全量的Pulsar主题消息

### 2.4.1.5 队列模式
![队列模式](src/main/resources/static/队列模式.jpg)

在多个消费者之间共享相同的订阅名称，此时多个消费者可共同消费一份全量消息

## 2.4.2 消费者
### 2.4.2.1 消费
```shell
bin/pulsar-client consume topic-test -s "subscription_test" --subscription-position Earliest
```
-s参数指定了当前使用的订阅的名称，这里为subscription_test。如果此订阅名从未被使用过，则服务端会创建一个新订阅。如果此订阅被使用过，则服务端会从上次消费的位置开始继续消费消息。参数--subscription-position代表该订阅在第一次被使用时需要初始化的位置。Earliest表示消费位置应该被初始化为主题中能消费的最早一条消息所在的位置。Latest表示消费位置应该被初始化为主题中最后一条消息的位置,如下图
![消费的位置和初始化的位置](src/main/resources/static/消费位置.jpg)

### 2.4.2.2 消息保留与消息过期
![消息保留策略与消息过期策略](src/main/resources/static/消息保留策略与消息过期策略.jpg)

（消息已过期和未过期反了？？？）

消息保留策略用于控制能够存储多少已被消费者确认的消息，消息过期策略用于为尚未确认的消息设置生存时间，本身不删除数据，只是更改状态为已确认

## 2.5 Pulsar逻辑架构
### 2.5.1 主题的配置管理
![租户、命名空间与主题](src/main/resources/static/租户、命名空间与主题.jpg)

租户、命名空间与主题之间关系如上图所示。

在主题视角下，租户代表基本的用户权限和集群权限，命名空间代表一类应用场景共同拥有的数据策略。例如在金融类业务下，命名空间中的主题可以牺牲一部分吞吐量来保障较高的可靠性。

资源隔离机制允许用户为命名空间分配资源，包括计算节点Broker和存储节点Bookie。pulsar-admin管理工具提供了ns-isolation-policy命令，该命令可以为命名空间分配Broker节点，并限制可用于分配的Broker。pulsar-admin管理工具的namespaces命令中的set-bookie-affinity-group子命令提供了Bookie节点的分配策略，该策略可以保证所有属于该命名空间的数据都存储在所需的Bookie节点中。

### 2.5.2 主题的数据流转
![生产者消息写入流程图](src/main/resources/static/生产者消息写入流程图.jpg)

用户将消息写入Pulsar时，客户端请求会被发送到管理该主题的Broker节点上，然后Broker节点会负责将消息写入Bookie节点中的某个Ledger中，在写入成功后Broker节点会将成功写入的状态返回给客户端

![消费者消息读取流程图](src/main/resources/static/消费者消息读取流程图.jpg)

在消费消息时，Pulsar会先尝试从Broker节点的缓存中读取消息，如果不能命中消息，则会从Bookie节点中读取消息，并最终将消息发送给消费者

### 2.5.3 主题的数据存储
![主题与Ledger的存储关系](src/main/resources/static/主题与Ledger的存储关系.jpg)

主题的数据在逻辑上被分解为多个片段（Segment）。片段在BookKeeper集群中对应着物理概念上的最小的分布单元—Ledger。每个Ledger都是一个独立的日志文件，一个主题中多个Ledger不会同时提供写入功能，同一时刻只会有一个Ledger处于开放状态并提供数据写入功能。Ledger在Pulsar中是最小的删除单元，因此我们不能删除单条记录，只能删除整个Ledger。

![消息的组成示意](src/main/resources/static/消息的组成示意.jpg)

每一条消息都会包含分区序号（partition-index）、Ledger序号（ledger-id）、Entry序号（entry-id）以及批内序号（batch-index）等信息，这些信息用来唯一确定一条消息

## 2.6 Pulsar物理架构
### 2.6.1 集群与实例
![Pulsar实例与Pulsar集群的关系](src/main/resources/static/Pulsar实例与Pulsar集群的关系.jpg)

实例代表一个或多个Pulsar集群的集合。实例中的集群之间可以相互复制数据，实现跨区域的数据备份。一个实例内的多个集群依赖Zookeeper集群实现彼此之间的协调任务，例如异地复制。
### 2.6.2 分层存储架构
![分层存储架构](src/main/resources/static/分层存储架构.jpg)

Pulsar的分层存储功能允许将较旧的积压数据卸载到可长期存储的系统中，例如Hadoop HDFS或Amazon S3。分层存储卸载（Offload）机制利用了这种面向数据段的架构。当请求卸载时，日志的数据段被一个一个地复制到分层存储系统中。命名空间策略可以配置为在达到阈值后自动卸载数据至分层存储系统中。一旦主题中的数据达到阈值，就会触发卸载操作。
### 2.6.3 核心组件与服务
1. Zookeeper：Zookeeper负责与分布式协调、部分元数据存储相关的工作
- 用于协调Pulsar实例中多个Pulsar集群的全局Zookeeper集群。在全局Zookeeper集群中，会存储Pulsar实例中多个Pulsar集群的信息，并在异地复制等多集群功能中发挥作用。
- 负责协调单个Pulsar集群的Zookeeper集群。这类Zookeeper集群会协调整个Pulsar集群的元数据管理和一致性工作，比如Broker节点状态、命名空间信息存储、命名空间Bundle分配、Ledger状态存储、订阅元数据信息以及负载均衡等。
2. Broker：生产消费服务与Pulsar管理服务
- 消息队列的生产与消费的基本功能。生产者可以连接到Broker节点发布消息，消费者可以连接到Broker节点来消费消息，在读取消息时如果能在Broker节点的缓存中命中消息，则直接从缓存中读取消息
- 以REST API的形式提供HTTP服务。Broker节点可以为租户和命名空间提供管理功能，为主题提供服务发现功能，为生产者和消费者提供管理接口，还可以为用户提供针对Pulsar Function、Pulsar I/O的管理功能。
3.  BookKeeper与Bookie：通过分布式预写日志实现的数据存储组件
![BookKeeper基本架构图](src/main/resources/static/BookKeeper基本架构图.jpg)
    
  在BookKeeper中除了Ledger外，还有日记账（Journal）、内存表（Memtable）、账目日志（Entry Log）、索引缓存（IndexCache）等概念

- 在数据写入BookKeeper的Ledger后，数据会首先写入日记账中，日记账代表Bookie中一个追加写入的预写日志，并被持久化到日记账磁盘中。在数据被持久化到日记账磁盘时，才会进行真正的数据写入操作。来自不同Ledger的日志数据被聚合并按顺序写入账目日志，数据存储的偏移量会作为指针保存在Ledger索引缓存中，以帮助实现快速查找功能。多个Ledger的数据会被顺序写入磁盘中，这确保了不会因为有过多的Ledger而影响磁盘的写入速度。但是这种方式需要维护额外的索引文件来保障数据读取速度。BookKeeper为每个Ledger分别创建了一个索引文件，这些索引文件记录了存储在记录日志中数据的偏移量。Ledger索引文件会被缓存在内存池中，这样可以更有效地管理磁盘磁头调度
4. 代理服务发现
   Pulsar代理在Pulsar中是一个无状态的代理组件。Pulsar客户端在连接服务端时，可以直接指定Broker的地址，也可以配置Pulsar代理的地址，然后由Pulsar代理将请求转发至Broker节点。Pulsar代理具有Broker节点无状态的性质，通过使用Pulsar代理服务发现组件，所有查找主题和数据连接都流经Pulsar代理。这样的代理可以在云原生环境下以多种模式公开，进而更好地发挥Pulsar的云原生特性。

## 03 Pulsar的基本操作
### 3.1 生产者开发
#### 3.1.1 消息：
- 键：
  键是消息中可选的一个配置参数，每个消息最多可以设置一个键的值。生产者发送数据到服务端时，由分区路由规则决定将该键值发送到哪个分区。主题压缩采集也会根据键值来压缩历史消息。
- 生产者名：
  顾名思义，就是生产者的名字。每条消息都会通过生产者名来记录每条消息是被哪个生产者发送的，所以该生产者名必须全局唯一，用户可以为生产者指定名称，也可以不指定，在不指定时系统会自动生成一个唯一的名称。
- 序列号：
  序列号由生产者分配，和生产者名一样，可以唯一定义一条消息。
- 发布时间：
  标记一条消息在生产者中发布的时间。
- 事件时间：
  区别于发布时间，事件时间代表该条消息在其生产设备上生成的时间。该属性可以在基于事件的流处理系统中进行计算处理。
- 属性：
  这是一个由用户自定义的属性集合。生产者可以在发送消息的同时附带一系列属性。这些属性可以在消费者中被访问。

![消息流转的流程图](src/main/resources/static/消息流转的流程图.jpg)
#### 3.1.2 消息流转流程：
1. 原始数据结合模式信息、元数据信息变成Pulsar中的消息。如果在生产者端配置了拦截器，则消息会被处理，而被处理后的消息也会被生产者端得到。
2. 根据生产者端的配置决定消息的发送形式。发送形式包括单条发送、分批发送（将多条消息打包为一条消息）、分块发送（将一条消息拆分为多条消息发送）。
3. Pulsar客户端会将写入数据的请求通过通信协议发送到服务端，服务端收到数据写入请求后进行写入操作，并在服务端确认写入持久化存储后，发送确认请求到生产者中
4. 客户端将该条消息从待处理消息队列中移除，并调用该消息之前配置的回调方法，以完成部分回调操作

```xml
<dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-client</artifactId>
    <version>2.7.1</version>
</dependency>
```

#### 3.1.3 获取pulsar客户端：
```text
PulsarClient.builder().serviceUrl(PULSAR_SERVICE_URL).build();
```
相关参数：
> - serviceUrl：
    必填参数。配置Pulsar服务访问的链接
> - numIoThreads：
    处理服务端连接的线程个数。默认为1。
> - numListenerThreads：
    主要用于消费者，处理消息监听和拉取的线程个数。默认为1。
> - statsIntervalSeconds：
    通过日志来打印客户端统计信息的时间间隔。默认为60秒。
> - connectionsPerBroker：
    在客户端处理Broker请求时，每个Broker对应建立多少个连接。默认为1。
> - memoryLimitBytes：
    客户端中的内存限制参数。默认为0。
> - operationTimeoutMs：
    网络通信超时时间。默认为30000毫秒。
> - keepAliveIntervalSeconds：
    每个客户端与服务端连接保持活动的间隔时间。在客户端底层会按照该参数周期性确认连接是否存活。默认为30秒。
> - connectionTimeoutMs：
    与服务端建立连接时的最大等待时间。默认为10000毫秒。
> - requestTimeoutMs：
    完成一次请求的最大超时时间。默认为60000毫秒。
> - concurrentLookupRequest：
    允许在每个Broker连接上并行发送的Lookup请求的数量，用来防止代理过载。Lookup请求用于查找管理某个主题的具体Broker地址。默认为5000个请求。
> - maxLookupRequest：
    一个Broker上允许的最大并发的Lookup请求数量。该参数与concurrentLookupRequest共同生效。默认为50000个请求。
> - maxNumberOfRejectedRequestPerConnection：
    当前连接关闭后，客户端在一定时间范围内（30秒）拒绝的最大请求数，超过该请求数后客户端会关闭旧的连接并创建新连接来连接不同的Broker。默认为50个连接。
> - useTcpNoDelay：
    这是一个网络通信底层参数，用于决定是否在连接上禁用Nagle算法。默认为True。
> - authPluginClassName：
    鉴权插件的类名。在服务端配置了鉴权时选择鉴权方式。
> - authParams：
    鉴权参数。和authPluginClassName一起使用，提供配置类型的鉴权参数。
> - useTls：
    是否在连接上使用TLS加密方式。
> - tlsTrustCertsFilePath：
    TLS加密的证书地址。
> - tlsAllowInsecureConnection：
    用于决定客户端是否接受Broker服务端的不可信证书。
> - tlsHostnameVerificationEnable：
    用于决定是否开启TLS主机名校验。

```java
public class ClientDemo {
    public static void main(String[] args) {
        ClientDemo.sendMessage();
    }

    public static void sendMessage() {
        PulsarClient clinet;
        try {
            clinet = PulsarClient.builder()
                    .serviceUrl(PulsarClientUtil.PULSAR_SERVICE_URL)
                    .ioThreads(1)
                    .listenerThreads(1)
                    .build();

            Producer<byte[]> producer = clinet.newProducer()
                    .topic("demo_topic")
                    .create();
            // 同步发送
            producer.send("同步消息".getBytes());
            // 异步发送
            producer.sendAsync("异步消息".getBytes())
                    .thenAccept(msgId -> System.out.println("发送成功"))
                    .exceptionally(throwable -> {
                        System.out.println("发送失败");
                        return null;
                    });
            producer.close();
            clinet.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    // 发送指定key的消息
    public static void sendKeyMsg() throws PulsarClientException {
        PulsarClient client = PulsarClientUtil.getClient();
        Producer<KeyValue<String, String>> keyTopic =
                client.newProducer(Schema.KeyValue(Schema.STRING, Schema.STRING)).topic("key_topic").create();
        keyTopic.newMessage().value(new KeyValue<>("key", "key msg")).send();
    }
}
```
生产者相关参数：
> - producerName：
为生产者命名，若不指定名称则会自动创建一个唯一的名称。
> - sendTimeoutMs：
写入消息的超时时间。如果在该时间范围内未收到服务端的确认回复则会抛出异常。默认为30000毫秒。
> - maxPendingMessages：
保存一个生产者待处理消息的队列，该队列中存放的是尚未被服务端确认写入的消息数量。默认为1000。当该队列存满之后将通过blockIfQueueFull参数决定如何处理。
> - blockIfQueueFull：
在发送消息到服务端时会将消息放在内存队列中，该参数控制在内存队列满了之后的处理行为。设为True则阻塞客户端的写入，设为False则不阻塞队列而是直接抛出异常。默认为False。
> - maxPendingMessagesAcrossPartitions：
在同一个主题下，不同分区内的多个生产者同用的最大待处理消息的总数量。与maxPendingMessages参数共同作用，最后生效的maxPendingMessages的值不得大于maxPendingMessagesAcrossPartitions除以分区数所得到的值。
> - messageRoutingMode：
针对分区主题的消息路由规则，决定一条消息实际被发送到哪个分区内。
> - hashingScheme：
针对分区主题的消息路由规则，指定一条消息的哈希函数。
> - cryptoFailureAction：
当加密失败时，生产者应该采取的发送行动。该参数的值为FAIL表示如果加密失败，则未加密的消息无法发送；该参数的值为SEND表示如果加密失败，则发送未加密的消息。默认为FA I L。
> - batchingEnabled：
是否允许批处理发送。
> - batchingMaxPublishDelayMicros：
发送批处理消息的最大延迟时间。
> - batchingMaxMessages：
发送批处理消息的最大数量。
> - compressionType：
消息压缩方式。

#### 3.1.4 数据发送路由规则:
1. 轮询路由模式
- 没有提供消息的键值，生产者会按照轮询策略跨所有分区发布消息
- 指定了一个键值，则分区的生产者会对该键值进行散列并将消息分配给特定的分区。
2. 单分区模式
- 未提供消息的键值，则生产者会随机选择一个分区并将所有消息发布到该分区。
- 指定了一个键值，则分区的生产者会对该键值进行哈希运算并将消息分配给特定的分区
3. 自定义分区模式
- 通过调用自定义的消息路由器来确定消息发往的分区
```java
public class RandomRouter implements MessageRouter {
    static Random random = new Random();
    @Override
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        int numPartitions = metadata.numPartitions();
        return random.nextInt(numPartitions);
    }
}
```

#### 3.1.5 分批发送和分块发送:
```text
// 批量发送
public static Producer<byte[]> getBatchProducer() throws PulsarClientException {
    PulsarClient client = getClient();
    return client.newProducer()
            .topic("batch-topic")
            .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
            .sendTimeout(10, TimeUnit.SECONDS)
            .enableBatching(true)
            .create();
}

// 大数据下一条数据分块发送
public static Producer<byte[]> getChunckProducer() throws PulsarClientException {
    PulsarClient client = getClient();
    return client.newProducer(Schema.BYTES)
            .topic("chunk-topic")
            .enableChunking(true)
            .enableBatching(false)
            .create();
}
```

### 3.1.6 生产者拦截器
```java
public class UpperConsumerInterceptor implements ProducerInterceptor {
    @Override
    public void close() {

    }

    @Override
    public boolean eligible(Message message) {
        if (message instanceof MessageImpl) {
            return (((MessageImpl) message).getSchema()) instanceof StringSchema;
        }
        return false;
    }

    @Override
    public Message beforeSend(Producer producer, Message message) {
        byte[] rawData = new String(message.getData()).toUpperCase().getBytes();
        return MessageImpl.create(((MessageImpl<?>) message).getMessageBuilder(), ByteBuffer.wrap(rawData), StringSchema.utf8());
    }

    @Override
    public void onSendAcknowledgement(Producer producer, Message message, MessageId messageId, Throwable throwable) {

    }
}
```

## 3.2 消费者开发
### 3.2.1 消费订阅
#### 3.2.1.1 普通消费
```java
public class ConsumerDemo {
    public static void main(String[] args) {
        receive();
    }
    public static Consumer<String> getConsumer() {
        PulsarClient client = PulsarClientUtil.getClient();
        try {
            return client.newConsumer(Schema.STRING)
                    .topic("demo_topic")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("test_sub")
                    .subscribe();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public static void receive() {
        Consumer<String> consumer = getConsumer();
        try {
            Message<String> msg = consumer.receive();
            System.out.println("收到消息：" + new String(msg.getData()));
            consumer.acknowledge(msg);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

    }
}
```
#### 3.2.1.2 异步接收与分批接收
```text
public static void asyncConsume() {
    Consumer<String> consumer = getConsumer();
    consumer.receiveAsync()
            .thenAccept((Message<String> msg) -> {
                System.out.println("receive:" + Arrays.toString(msg.getData()));
                try {
                    consumer.acknowledge(msg);
                } catch (PulsarClientException e) {
                    consumer.negativeAcknowledge(msg);
                }
            });
}

public static void batchConsume() throws PulsarClientException {
    Consumer<String> consumer = getConsumer();
    Messages<String> messages = consumer.batchReceive();
    for (Message<String> message : messages) {
        System.out.println("receive:" + Arrays.toString(message.getData()));
        // 单消息确认
        // try {
        //     consumer.acknowledge(message);
        // } catch (PulsarClientException e) {
        //     consumer.negativeAcknowledge(message);
        // }
    }
    // 批量确认
    consumer.acknowledge(messages);
}
```
消费者配置参数：
> - consumerName：
消费者的名字，类似生产者命名，是消费者的唯一身份凭证，需要保证全局唯一。若不指定，则系统会自动生成全局唯一的消费者命名。
> - topicNames：
topicName的集合，表示该消费者要消费的一组主题。
> - topicsPattern：
主题模式，可以按照正则表达式的规则匹配一组主题。
> - patternAutoDiscoveryPeriod：
和topicsPattern一起使用，表示每隔多长时间重新按照模式匹配主题。
> - regexSubscriptionMode：
正则订阅模式的类型。使用正则表达式订阅主题时，你可以选择订阅哪种类型的主题。PersistentOnly表示只订阅持久性主题；NonPersistentOnly表示仅订阅非持久性主题；AllTopics表示订阅持久性和非持久性两种主题。
> - subscriptionType：
定义订阅模式，订阅模式分为独占、故障转移、共享、键共享4种。
> - subscriptionInitialPosition：
提交了订阅请求，但是在当前时刻订阅未被创建，服务端会创建该订阅，该参数用于设定新创建的订阅中消费位置的初始值。
> - priorityLevel：
订阅优先级。在共享订阅模式下分发消息时，服务端会优先给高优先级的消费者发送消息。在拥有最高优先级的消费者可以接收消息的情况下，所有消息都会被发送到该消费者。当拥有最高优先级的消费者不能接收消息时，服务端才会考虑下一个优先级消费者。
> - receiverQueueSize：
用于设置消费者接收队列的大小，在应用程序调用Receive方法之前，消费者会在内存中缓存部分消息。该参数用于控制队列中最多缓存的消息数。配置高于默认值的值虽然会提高使用者的吞吐量，但会占用更多的内存。
> - maxTotalReceiverQueueSizeAcrossPartitions：
用于设置多个分区内最大内存队列的长度，与receiverQueueSize一同生效。当达到任意一个队列长度限制时，所有接收队列都不能再继续接收数据了。
下面是与消息确认相关的参数。
> - acknowledgementsGroupTimeMicros：
用于设置消费者分批确认的最大允许时间。默认情况下，消费者每100毫秒就会向服务端发送确认请求。将该时间设置为0会立即发送确认请求。
> - ackTimeoutMillis：
未确认消息的超时时间。
> - negativeAckRedeliveryDelayMicros：
用于设置重新传递消息的延迟时间。客户端在请求重新发送未能处理的消息时，不会立刻发送，而是会有一段时间的延迟。当应用程序使用negativeAcknowledge方法时，失败的消息会在该时间后重新发送。
> - tickDurationMillis：
ack-timeout重新发送请求的时间粒度。
其他高级特性的配置与使用。
> - readCompacted：
在支持压缩的主题中，如果启用readCompacted，消费者会从压缩的主题中读取消息，而不是读取主题的完整消息积压。消费者只能看到压缩主题中每个键的最新值。我们会在6.5节对此进行详细介绍。
> - DeadLetterPolicy：
用于启动消费者的死信主题。默认情况下，某些消息可能会多次重新发送，甚至可能永远都在重试中。通过使用死信机制，消息具有最大重新发送计数。当超过最大重新发送次数时，消息被发送到死信主题并自动确认。
> - replicateSubscriptionState：
如果启用了该参数，则订阅状态将异地复制到集群。
### 3.2.2 数据确认
消息一旦被生产者成功写入服务端，该消息会被永久存储，只有在所有订阅都确认后该消息才会被允许删除

#### 3.2.2.1 独立确认和累计确认
采用独立确认时，消费者需要确认每条消息并向服务端发送确认请求。采用累积确认时，消费者只需要确认它收到的最后一条消息就可确认该条消息及之前的消息。在非共享订阅模式中，包括独占模式与灾备模式可使用累计确认。

累计确认：
```text
client.newConsumer(Schema.STRING)
          .topic("demo_topic")
          .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
          .subscriptionName("group_sub")
          .acknowledgmentGroupTime(100, TimeUnit.MICROSECONDS)
          .subscribe();
```

#### 3.2.2.2 确认超时与重试
有参数ackTimeout，在该参数内没被确认的消息会被重新发送，如下每5s重新发送一次
```text
// 超时,不确认每5s重新消费一次。被否认确认的消息会在固定超时时间后重新发送，
// 重新发送的周期由negativeAckRedeliveryDelay参数控制，默认为1min。
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("demo_topic")
    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
    .subscriptionName("group_sub")
    .ackTimeout(5, TimeUnit.SECONDS)
    .acknowledgmentGroupTime(100, TimeUnit.MICROSECONDS)
    .subscribe();
while (true) {
    Message<String> msg = consumer.receive();
    System.out.println(msg.getData());
    // consumer.acknowledge(msg);
}

// 重试，在未指定死信配置（DeadLetterPolicy）时，客户端会自动以当前的主题名和订阅名生成默认重试主题
// 规则为${topic_name}-${subscription_name}-RETRY。
// 在调用reconsumeLater方法请求重新写入一条消息时，系统会自动发送确认请求到服务端
// 然后在配置的重试主题下，重新写入一条消息。在超过指定延迟时间后，客户端会重新消费该消息。
Consumer<String> consumer = client.newConsumer(Schema.STRING)
        .topic("demo_topic")
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
        .subscriptionName("retry_sub")
        .enableRetry(true)
        .subscribe();
while (true) {
    Message<String> msg = consumer.receive();
    System.out.println(msg.getData());
    consumer.reconsumeLater(msg, 10, TimeUnit.SECONDS);
```

#### 3.2.2.3 消费者拦截器和监听器
与生产者拦截器相似，要想实现消费者拦截器，需要先实现ConsumerInterceptor接口。该接口中有以下几个方法。
> - beforeConsume：
在消息到达receive方法前进行拦截。
> - onAcknowledge：
在消费者向服务端发送调用请求前被调用。
> - onAcknowledgeCumulative：
在消费者向服务端发送累计确认请求前被调用。
> - onAckTimeoutSend：
当消息确认超时后，向服务端发送“重新发送请求”前被调用。
> - onNegativeAcksSend：
在消费者向服务端周期性发送否认确认请求前被调用


Pulsar的消费者客户端提供了两种类型的监听器—ConsumerEventListener和MessageListener。
1. ConsumerEventListener：用来监听消费者状态变动的监听器，可在故障转移模式下发生分区分配策略变化时监听状态的变动。
- becameActive在当前消费者获取到一个分区的消费权利时被调用
- becameInactive在当前消费者没有分区消费权利时被调用
2. MessageListener：在使用消息监听器时，receive方法不再提供服务
- 此时调用receive方法会收到客户端异常“Cannot use receive() whena listener has been set”（设置监听器后不能再使用receive()）
- 此时每条发向当前消费者的消息都会调用MessageListener.received方法

### 3.2.3 Reader开发
Pulsar的Reader接口可以使我们通过应用程序手动管理访问游标。当使用Reader接口访问主题时，需要指定Reader在连接到主题时开始读取的消息的位置，例如最早和最后可以访问到的有效消息位置

```text
Reader<String> reader = client.newReader(Schema.STRING)
        .topic("demo_topic")
        .startMessageId(MessageId.earliest)
        .create();
while (true) {
    Message<String> msg = reader.readNext();
    System.out.println(msg.getData());
}
```

### 3.2.4 模式管理
模式是一种数据类型的定义方法，提供了统一的类型管理和序列化（或反序列化）方式，可以减轻用户维护类型安全的工作量。通过使用模式，Pulsar客户端会强制执行类型安全检查，并确保生产者和消费者保持同步。

- **原始类型模式**
1. 字节数组（BYTES）：默认的模式格式。对应Java中的byte[]、ByteBuffer类型。
2. 布尔类型（BOOLEAN）：对应Java Boolean类型。
3. 整数类型（INT8、INT16、INT32、INT64）：按照数据所占字节不同又可分为8位、16位、32位和64位，对应Java的byte、short、int、long类型。
4. 浮点类型（FLOAT、DOUBLE）：分为32位的单浮点数和64位的双浮点数，对应Java的float和double。
5. 字符串（STRING）：Unicode字符串，对应着Java中的String类型。
6. 时间戳（TIMESTAMP、DATE、TIME、INSTANT、LOCAL_DATE、LOCAL_TIME、LOCAL_DATE_TIME）：时间字段类型，对应Java中的java.sql.Timestamp、java.sql.Time、java.util.Date、java.time.Instant、java.time.LocalDate、java.time.LocalDateTime、java.time.LocalTime类型。其中INSTANT代表时间线上的单个瞬时点，精度为纳秒。

Pulsar中对基本数据类型模式的使用方式是，在创建生产者和消费者时传入原始类型模式：
```text
Producer<String> producer = clinet.newProducer(Schema.STRING)
        .topic("demo_topic")
        .create();


Reader<String> reader = client.newReader(Schema.STRING)
        .topic("demo_topic")
        .startMessageId(MessageId.earliest)
        .create();
```

- **复杂类型模式**

复杂类型模式分为两类：键值对类型模式和结构体类型模式。

a. 键值对类型模式: 键值有两种编码形式—内联编码和分离编码。内联编码会将键和值在消息主体中一起编码；分离编码会将键编码在消息密钥中，将值编码在消息主体中。

```text
Schema<KeyValue<Integer, String>> inlineSchema = Schema.KeyValue(Schema.INT32, Schema.STRING, KeyValueEncodingType.INLINE);

Schema<KeyValue<Integer, String>> schema = Schema.KeyValue(Schema.INT32, Schema.STRING);
```

b. 结构体类型模式: 可以让用户很方便地传输Java对象

目前Pulsar支持AvroBaseStruct-Schema和ProtobufNativeSchema两种结构体类型模式。AvroBaseStructSchema支持Avro-Schema、JsonSchema和ProtobufSchema。利用Pulsar支持的几种模式可以预先定义结构体架构，它既可以是Java中的简单的Java对象（POJO）、Go中的结构体，又可以是Avro或Protobuf工具生成的类。ProtobufNativeSchema使用原生Protobuf协议的格式来进行序列化。

```text
Producer<DemoData> producer1 = client.newProducer(JSONSchema.of(DemoData.class)).create();

Producer<DemoData> producer2 = client.newProducer(AvroSchema.of(DemoData.class)).create();

// ProtobufSchema使用的对象需要继承GeneratedMessageV3
Producer<ProtocolData> producer3 = client.newProducer(ProtobufSchema.of(ProtocolData.class)).create();

Producer<ProtocolData> producer4 = client.newProducer(ProtobufNativeSchema.of(ProtocolData.class)).create();
```


