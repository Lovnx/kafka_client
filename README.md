# `kafka_client介绍`
## **背景：**


## **目标：**


## **设计：**
### **生产者**
##### 支持官网的生产者模型:
![image](./image/生产者模型.png)<br>

**本项目事务支持：**<br>
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


### **项目结构依赖**

| 名称                | 版本    
| -----             |-----:   
| spring            | 5.1.5.RELEASE      
| commons-lang3     | 3.9     
| fastjson          | 1.2.58   
| lombok            | 1.18.6   
| kafka-clients     | 2.2.0   
| guava             | 27.1-jre    
