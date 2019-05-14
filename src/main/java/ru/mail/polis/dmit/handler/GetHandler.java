package ru.mail.polis.dmit.handler;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.dmit.KVDaoServiceImpl;
import ru.mail.polis.dmit.RF;
import ru.mail.polis.dmit.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class GetHandler extends RequestHandler {
    private List<Value> values = new ArrayList<>();

    public GetHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id) {
        super(methodName, dao, rf, id, null);
    }

    @Override
    public Response onProxied() throws NoSuchElementException {
        Value value = dao.internalGet(id.getBytes());
        Response response = new Response(Response.OK, value.getData());
        response.addHeader(TIMESTAMP + String.valueOf(value.getTimestamp()));
        response.addHeader(STATE + value.getState().name());
        return response;
    }

    @Override
    public Callable<Boolean> ifMe() {
        return () -> {
            Value value = dao.internalGet(id.getBytes());
            values.add(value);
            return true;
        };
    }

    @Override
    public Callable<Boolean> ifNotMe(HttpClient client) {
        return () -> {
            final Response response = client.get(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id, KVDaoServiceImpl.PROXY_HEADER);
            values.add(new Value(response.getBody(), Long.valueOf(response.getHeader(TIMESTAMP)),
                    Value.State.valueOf(response.getHeader(STATE))));
            return true;
        };
    }

    @Override
    public Response onSuccess(int acks) throws NoSuchElementException {
        Value max = values
                .stream()
                .max(Comparator.comparing(Value::getTimestamp))
                .get();
        if(max.getState() == Value.State.PRESENT){
            return success(Response.OK, acks, max.getData());
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }


    @Override
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        return super.getResponse(futures);
    }
}