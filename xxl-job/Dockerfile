FROM python:latest
MAINTAINER kang
WORKDIR /bigdisk/taskcreate
ENV TZ=PRC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
ADD . /bigdisk/taskcreate

RUN chmod +x /bigdisk/taskcreate/startup.sh
ENTRYPOINT ["sh","-c","/bigdisk/taskcreate/startup.sh"]
