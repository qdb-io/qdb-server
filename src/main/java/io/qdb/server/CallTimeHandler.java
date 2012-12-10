package io.qdb.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Logs and times all calls to a proxy.
 */
public class CallTimeHandler implements InvocationHandler {

    private Logger log = LoggerFactory.getLogger(CallTimeHandler.class);

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (log.isDebugEnabled()) {
            long start = System.currentTimeMillis();
            Object ans = method.invoke(proxy, args);
            log.debug(method.getName() + " " + (start - System.currentTimeMillis()) + " ms");
            return ans;
        } else {
            return method.invoke(proxy, args);
        }
    }
}
