<!-- TOC -->autoauto- [`kafka_client介绍`](#kafka_client介绍)auto    - [**背景：**](#背景)auto    - [**目标：**](#目标)auto    - [**设计：**](#设计)auto        - [**生产者**](#生产者)auto                - [支持官网的生产者模型:](#支持官网的生产者模型)auto        - [**消费者**](#消费者)auto                - [支持官网的消费者模型:](#支持官网的消费者模型)auto                - [本次实现的消费者模型:](#本次实现的消费者模型)auto        - [**项目结构**](#项目结构)auto                - [消费者消息滞留监控思路:](#消费者消息滞留监控思路)auto                - [测试代码](#测试代码)auto        - [**项目结构依赖**](#项目结构依赖)auto        - [**待做功能**](#待做功能)autoauto<!-- /TOC -->

# `kafka_client介绍`
## **背景：**
kafka 具有吞吐量高,高压高堆积的特点,从 0.8.x 版本到 0.10.x 1.x 2.x版本,kafka本身发生了多次变革,修正了大多bug也引入了新功能和特性
,其client的使用也变的复杂多样,不同的人可能有不同的使用方式,由于开发的使用不当导致消息丢失,消息消费跟踪日志不足的事情屡屡发生,同时kafka消费者端没有实现监控功能
当发生消息丢失或者消息滞留的情况下，消费者无法及时发觉，导致消费者被压垮。

## **目标：**
本项目的目的,通过和spring的整合,提供一套配置完成开箱即用的client封装给开发者,通过配置的方式体验kafka的各种特性,
同时topic和topic之间的消费又保持着一定的隔离性,彼此直接不干预,同时提供生成者消费者监控,和环境日志的记录,保证出现异常的时候开发有足够的日志信息 </br>
目标如下: </br>
1、发挥新版kafka api的特性(如事物，幂等消息) </br>
2、整合spring尽可能做到开箱即用 </br>
3、从push模式转换成poll模式，增加吞吐量,同时保护consumer安全性 </br>
4、整合公司现有的监控平台，例如：cat，当发生消息堆积的时候及时发起告警功能 </br>
5、增加特性,例如JVM内部广播功能 </br>
6、利用新版kafka特点降低对zookeeper的依赖 </br>
7、降低消息丢失的风险 </br>


## **设计：**
### **生产者**
##### 支持官网的生产者模型:
![image](./image/生产者模型.png)<br>

**本项目生产者模型：**<br>
![image](./image/生产者模型3.jpg)<br>

开启事物方式：<br>
![image](./image/事务1.png)<br>
业务代码示例：<br>
![image](./image/事务2.png)<br>
如果有异常需要回滚，那么两次消息都将不会发送出去

### **消费者**
##### 支持官网的消费者模型:
![image](./image/消费者模型.png)<br>

![image](./image/消费者模型2.jpg)<br>


##### 本次实现的消费者模型:

![image](./image/消费者模型6.jpg)<br>

### **项目结构**
![image](./image/项目分包.png)<br>

![image](./image/项目核心类.png)<br>

![image](./image/消费者监控.png)<br>

##### 消费者消息滞留监控思路:
![image](./image/监控思路.png)<br>


##### 测试代码
![image](./image/测试代码.png)<br>

### **项目结构依赖**

| 名称                | 版本    
| -----             |-----:   
| spring            | 5.1.5.RELEASE      
| commons-lang3     | 3.9     
| fastjson          | 1.2.58   
| lombok            | 1.18.6   
| kafka-clients     | 2.2.0   
| guava             | 27.1-jre    

### **待做功能**
1、和Cat整合，引入监控告警功能 <br>
2、消息上下游链路监控 <br>
3、图片流传输(机器学习使用)【方式通过partition进行图片分块,同一张图片发给同一个partition】 <br>