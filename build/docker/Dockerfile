FROM openjdk:8-jre-alpine

ENV APPLICATION_USER bigben
RUN adduser -D -g '' $APPLICATION_USER
ENV APP_ROOT /dist
RUN if [ -d "$APP_ROOT" ]; then rm -Rf $APP_ROOT; fi
RUN mkdir $APP_ROOT
RUN chown -R $APPLICATION_USER $APP_ROOT
USER $APPLICATION_USER
COPY ./build/bin/bigben.jar $APP_ROOT/bigben.jar
COPY ./build/docker/start.sh $APP_ROOT/start.sh
USER root
RUN chmod +x $APP_ROOT/start.sh
WORKDIR $APP_ROOT
EXPOSE 8080 5701

CMD ["sh", "-c", "$APP_ROOT/start.sh"]