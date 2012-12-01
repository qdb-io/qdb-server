package io.qdb.server.controller;

import java.io.IOException;

public interface Controller {

    public void handle(Call call) throws IOException;

}
