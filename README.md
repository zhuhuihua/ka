# ka
kafka-study
kafka restful
需要移除log4j-over-slf4j,logback*.jar, servlet2.5.jar三个包，由于kafka和spring boot 
使用了不同的log4j实现，导致spring boot启动不成功，spring boot 使用servlet3.x的jar,不删除servlet2.5.jar也会导致spring boot启动不成功
运行项目之前需要先把src/main/dist和src/main/resource两个source folder加到build path下，
