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

public class GetHandler extends RequestHandler {
    private List<Value> values = new ArrayList<>();

    public GetHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id) {
        super(methodName, dao, rf, id, null);
    }

    @Override
    public Response onProxied() throws NoSuchElementException {
        Value value = dao.internalGet(id.getBytes());
        if(value.getState() == Value.State.PRESENT || value.getState() == Value.State.ABSENT){
            Response response = new Response(Response.OK, value.getData());
            response.addHeader(TIMESTAMP + String.valueOf(value.getTimestamp()));
            response.addHeader(STATE + value.getState().name());
            return response;
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public boolean ifMe() {
        Value value = dao.internalGet(id.getBytes());
        if(value.getState() == Value.State.PRESENT || value.getState() == Value.State.ABSENT){
            values.add(value);
        } else if(value.getState() == Value.State.REMOVED){
            throw new NoSuchElementException();
        }
        return true;
    }

    @Override
    public boolean ifNotMe(HttpClient client) throws InterruptedException, PoolException, HttpException, IOException, NoSuchElementException {
        final Response response = client.get(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id, KVDaoServiceImpl.PROXY_HEADER);

        if(response.getStatus() == 200){
            values.add(new Value(response.getBody(), Long.valueOf(response.getHeader(TIMESTAMP)),
                    Value.State.valueOf(response.getHeader(STATE))));

        } else if(response.getStatus() == 404){
            throw new NoSuchElementException();
        }
        return true;
    }

    @Override
    public Response onSuccess(int acks) throws NoSuchElementException{
        if(values.stream().anyMatch(value -> value.getState() == Value.State.PRESENT)){
            Value max = values
                    .stream()
                    .max(Comparator.comparing(Value::getTimestamp))
                    .get();
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
    public Response getResponse(int acks) {
        return super.getResponse(acks);
    }
}
