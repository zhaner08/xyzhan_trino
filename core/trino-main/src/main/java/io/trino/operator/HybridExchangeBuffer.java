/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.exchange.ExchangeContextInstance;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.TaskId;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Multimaps.asMap;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class HybridExchangeBuffer
        implements DirectExchangeBuffer
{
    private static final Logger log = Logger.get(HybridExchangeBuffer.class);
    private static final Duration SINK_INSTANCE_HANDLE_GET_TIMEOUT = Duration.succinctDuration(60, TimeUnit.SECONDS);
    private static final int DEFAULT_MAX_PAGE_SIZE_IN_BYTES = 1024 * 1024; // 1MB

    private final Executor executor;
    private final DataSize deduplicationBufferSize;
    private final DataSize maxBufferedBytes;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final QueryId queryId;
    private final Span parentSpan;
    private final ExchangeId exchangeId;

    @GuardedBy("this")
    private final Set<TaskId> allTasks = new HashSet<>();
    @GuardedBy("this")
    private boolean noMoreTasks;
    @GuardedBy("this")
    private final Set<TaskId> successfulTasks = new HashSet<>();
    @GuardedBy("this")
    private final Map<TaskId, Throwable> failedTasks = new HashMap<>();
    @GuardedBy("this")
    private int maxAttemptId;

    @GuardedBy("this")
    private final ListMultimap<TaskId, Slice> deduplicationBuffer = ArrayListMultimap.create();
    @GuardedBy("this")
    private long deduplicationBufferRetainedSizeInBytes;

    @GuardedBy("this")
    private final Queue<Slice> streamingBuffer = new ArrayDeque<>();
    @GuardedBy("this")
    private final AtomicLong streamingBufferRetainedSizeInBytes = new AtomicLong();
    @GuardedBy("this")
    private volatile long maxStreamingBufferRetainedSizeInBytes;
    @GuardedBy("this")
    private final Queue<SettableFuture<Void>> blocked = new ArrayDeque<>();

    @GuardedBy("this")
    private ExchangeManager exchangeManager;
    @GuardedBy("this")
    private Exchange exchange;
    @GuardedBy("this")
    private ExchangeSinkHandle sinkHandle;
    @GuardedBy("this")
    private ExchangeSink exchangeSink;
    @GuardedBy("this")
    private SliceOutput writeBuffer;

    @GuardedBy("this")
    private int bufferedPageCount;
    @GuardedBy("this")
    private long spilledBytes;
    @GuardedBy("this")
    private int spilledPageCount;

    @GuardedBy("this")
    private boolean inputFinished;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private Throwable failure;

    public HybridExchangeBuffer(
            Executor executor,
            DataSize deduplicationBufferSize,
            DataSize maxBufferedBytes,
            ExchangeManagerRegistry exchangeManagerRegistry,
            QueryId queryId,
            Span parentSpan,
            ExchangeId exchangeId)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.deduplicationBufferSize = requireNonNull(deduplicationBufferSize, "deduplicationBufferSize is null");
        this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.parentSpan = requireNonNull(parentSpan, "parentSpan is null");
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
    }

    @Override
    public synchronized ListenableFuture<Void> isBlocked()
    {
        if (!streamingBuffer.isEmpty() || isFailed() || (noMoreTasks && allTasks.isEmpty())) {
            return immediateVoidFuture();
        }
        SettableFuture<Void> callback = SettableFuture.create();
        blocked.add(callback);
        return nonCancellationPropagating(callback);
    }

    @Override
    public synchronized Slice pollPage()
    {
        throwIfFailed();

        if (closed) {
            return null;
        }

        Slice page = streamingBuffer.poll();
        if (page != null) {
            long retained = streamingBufferRetainedSizeInBytes.addAndGet(-page.getRetainedSize());
            checkState(retained >= 0, "unexpected streamingBufferRetainedSizeInBytes: %s", retained);
        }
        return page;
    }

    @Override
    public synchronized void addTask(TaskId taskId)
    {
        if (closed) {
            return;
        }
        checkState(!noMoreTasks, "no more tasks are expected");
        allTasks.add(taskId);
        maxAttemptId = max(maxAttemptId, taskId.getAttemptId());
    }

    @Override
    public synchronized void addPages(TaskId taskId, List<Slice> pages)
    {
        if (closed) {
            return;
        }

        if (inputFinished) {
            // ignore extra pages after input is marked as finished
            return;
        }

        long pagesRetainedSizeInBytes = getRetainedSizeInBytes(pages);

        // If we're still in deduplication mode and have space in the deduplication buffer
        if (exchangeSink == null && deduplicationBufferRetainedSizeInBytes + pagesRetainedSizeInBytes <= deduplicationBufferSize.toBytes()) {
            deduplicationBuffer.putAll(taskId, pages);
            deduplicationBufferRetainedSizeInBytes += pagesRetainedSizeInBytes;
            bufferedPageCount += pages.size();
            return;
        }

        // If we need to switch to streaming mode
        if (exchangeSink == null) {
            verify(exchangeManager == null, "exchangeManager is not expected to be initialized");
            verify(exchange == null, "exchange is not expected to be initialized");
            verify(sinkHandle == null, "sinkHandle is not expected to be initialized");
            verify(writeBuffer == null, "writeBuffer is not expected to be initialized");

            exchangeManager = exchangeManagerRegistry.getExchangeManager();
            exchange = exchangeManager.createExchange(new ExchangeContextInstance(queryId, exchangeId, parentSpan), 1, true);

            sinkHandle = exchange.addSink(0);
            exchange.noMoreSinks();
            ExchangeSinkInstanceHandle sinkInstanceHandle;
            try {
                sinkInstanceHandle = exchange.instantiateSink(this.sinkHandle, 0).get(SINK_INSTANCE_HANDLE_GET_TIMEOUT.toMillis(), MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            exchangeSink = exchangeManager.createSink(sinkInstanceHandle);
            writeBuffer = new DynamicSliceOutput(DEFAULT_MAX_PAGE_SIZE_IN_BYTES);

            // Move all pages from deduplication buffer to streaming buffer
            for (Map.Entry<TaskId, List<Slice>> entry : asMap(deduplicationBuffer).entrySet()) {
                streamingBuffer.addAll(entry.getValue());
                long retained = streamingBufferRetainedSizeInBytes.addAndGet(getRetainedSizeInBytes(entry.getValue()));
                maxStreamingBufferRetainedSizeInBytes = max(maxStreamingBufferRetainedSizeInBytes, retained);
            }
            deduplicationBuffer.clear();
            deduplicationBufferRetainedSizeInBytes = 0;
        }

        // Add new pages to streaming buffer
        streamingBuffer.addAll(pages);
        long retained = streamingBufferRetainedSizeInBytes.addAndGet(pagesRetainedSizeInBytes);
        maxStreamingBufferRetainedSizeInBytes = max(maxStreamingBufferRetainedSizeInBytes, retained);
        bufferedPageCount += pages.size();

        // Unblock consumers
        unblock(pages.size());
    }

    private static long getRetainedSizeInBytes(List<Slice> pages)
    {
        long result = 0;
        for (Slice page : pages) {
            result += page.getRetainedSize();
        }
        return result;
    }

    @Override
    public synchronized void taskFinished(TaskId taskId)
    {
        if (closed) {
            return;
        }
        checkState(allTasks.contains(taskId), "taskId not registered: %s", taskId);
        successfulTasks.add(taskId);
        if (noMoreTasks && successfulTasks.size() == allTasks.size()) {
            unblockAll();
        }
    }

    @Override
    public synchronized void taskFailed(TaskId taskId, Throwable t)
    {
        if (closed) {
            return;
        }
        checkState(allTasks.contains(taskId), "taskId not registered: %s", taskId);
        failedTasks.put(taskId, t);
        if (noMoreTasks && successfulTasks.size() + failedTasks.size() == allTasks.size()) {
            unblockAll();
        }
    }

    @Override
    public synchronized void noMoreTasks()
    {
        if (closed) {
            return;
        }
        noMoreTasks = true;
        if (successfulTasks.size() + failedTasks.size() == allTasks.size()) {
            unblockAll();
        }
    }

    @Override
    public synchronized boolean isFinished()
    {
        return failure == null && (closed || (noMoreTasks && successfulTasks.size() + failedTasks.size() == allTasks.size()));
    }

    @Override
    public synchronized boolean isFailed()
    {
        return failure != null;
    }

    @Override
    public synchronized long getRemainingCapacityInBytes()
    {
        if (exchangeSink == null) {
            return deduplicationBufferSize.toBytes() - deduplicationBufferRetainedSizeInBytes;
        }
        return maxBufferedBytes.toBytes() - streamingBufferRetainedSizeInBytes.get();
    }

    @Override
    public synchronized long getRetainedSizeInBytes()
    {
        long result = deduplicationBufferRetainedSizeInBytes + streamingBufferRetainedSizeInBytes.get();
        if (exchangeSink != null) {
            result += exchangeSink.getMemoryUsage();
        }
        if (writeBuffer != null) {
            result += writeBuffer.getRetainedSize();
        }
        return result;
    }

    @Override
    public synchronized long getMaxRetainedSizeInBytes()
    {
        return max(maxStreamingBufferRetainedSizeInBytes, deduplicationBufferRetainedSizeInBytes);
    }

    @Override
    public synchronized int getBufferedPageCount()
    {
        return bufferedPageCount;
    }

    @Override
    public synchronized long getSpilledBytes()
    {
        return spilledBytes;
    }

    @Override
    public synchronized int getSpilledPageCount()
    {
        return spilledPageCount;
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        closeAndUnblock();
    }

    @GuardedBy("this")
    private void fail(Throwable failure)
    {
        this.failure = failure;
        closeAndUnblock();
    }

    @GuardedBy("this")
    private void throwIfFailed()
    {
        if (failure != null) {
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
    }

    @GuardedBy("this")
    private void closeAndUnblock()
    {
        try (Closer closer = Closer.create()) {
            if (exchangeSink != null) {
                closer.register(() -> exchangeSink.finish());
            }
            if (exchange != null) {
                closer.register(() -> exchange.close());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            unblockAll();
        }
    }

    private void unblock(int count)
    {
        for (int i = 0; i < count; i++) {
            SettableFuture<Void> callback = blocked.poll();
            if (callback != null) {
                executor.execute(() -> callback.set(null));
            }
        }
    }

    private void unblockAll()
    {
        SettableFuture<Void> callback;
        while ((callback = blocked.poll()) != null) {
            final SettableFuture<Void> finalCallback = callback;
            executor.execute(() -> finalCallback.set(null));
        }
    }
} 