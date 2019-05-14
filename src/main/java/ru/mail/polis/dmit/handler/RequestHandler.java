package ru.mail.polis.dmit.handler;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.dmit.KVDaoImpl;
import ru.mail.polis.dmit.RF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.*;


public abstract class RequestHandler {
    private final String methodName;
    @NotNull
    final KVDaoImpl dao;
    @NotNull
    private final RF rf;
    final String id;
    final byte[] value;

    final String TIMESTAMP = "timestamp";
    final String STATE = "state";

    private Logger log = LoggerFactory.getLogger(RequestHandler.class);


    RequestHandler(String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, byte[] value) {
        this.methodName = methodName;
        this.dao = (KVDaoImpl) dao;
        this.rf = rf;
        this.id = id;
        this.value = value;
    }


    public abstract Response onProxied() throws NoSuchElementException;


    public abstract Callable<Boolean> ifMe() throws IOException;


    public abstract Callable<Boolean> ifNotMe(HttpClient client);


    abstract Response onSuccess(int acks);


    abstract Response onFail(int acks);


    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        int acks = 0;

        for (Future<Boolean> future : futures) {
            try {
                if (future.get()) {
                    acks++;
                    if (acks >= rf.getAck()) {
                        return onSuccess(acks);
                    }
                }
            } catch (ExecutionException | InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

        return onFail(acks);
    }


    Response gatewayTimeout(int acks) {
        log.info("Операция " + methodName + " не выполнена, acks = " + acks + " ; требования - " + rf);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }


    Response success(String responseName, int acks, byte[] body) {
        log.info("Операция " + methodName + " выполнена успешно в " + acks + " нодах; требования - " + rf);
        return new Response(responseName, body);
    }
}