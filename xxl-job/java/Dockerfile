# 使用本地 JDK 路径
FROM openjdk:17-jdk

MAINTAINER kang

WORKDIR /app

ENV TZ=PRC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ADD xxl-job.jar /app/xxl-job.jar

#暴露端口作为微服务
EXPOSE 6588

ENTRYPOINT ["sh","-c","java -jar /app/xxl-job.jar"]