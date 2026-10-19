package com.lawyus.dataetl.connector.spi;

import java.util.Iterator;

/**
 * 可关闭的迭代器：流式游标读取时，worker 消费完（或提前取消）必须 {@link #close()} 释放 JDBC {@code ResultSet}/{@code Connection}。
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    @Override
    void close();
}
