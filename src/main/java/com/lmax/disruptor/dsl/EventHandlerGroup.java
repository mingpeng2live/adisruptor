/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor.dsl;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WorkHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A group of {@link EventProcessor}s used as part of the {@link Disruptor}.
 *
 * @param <T> the type of entry used by the event processors.
 */
public class EventHandlerGroup<T>
{
    private final Disruptor<T> disruptor;
    private final ConsumerRepository<T> consumerRepository;
    private final Sequence[] dependencySequences; // 依赖的序列
    private final List<Sequence> nextDependencySequenceList = new ArrayList<Sequence>(); // 下一步需要的依赖的序列
    private Sequence[] nextDependencySequence;

    EventHandlerGroup(
        final Disruptor<T> disruptor,
        final ConsumerRepository<T> consumerRepository,
        final Sequence[] sequences) {

        this.disruptor = disruptor;
        this.consumerRepository = consumerRepository;
        this.dependencySequences = Arrays.copyOf(sequences, sequences.length);
        this.add(sequences);
    }

    Sequence[] add(Sequence... sequence) {
        this.nextDependencySequenceList.addAll(Arrays.asList(sequence));
        return sequence;
    }

    /** 增加一组新的handler到当前组中,且不依赖任何其他的组和handler, 用于disruptor中消费者第一组构建*/
    public EventHandlerGroup<T> addNew(final EventHandler<? super T>... eventHandlers){
        return disruptor.addCreateEventProcessors(this, new Sequence[0], eventHandlers);
    }
    /** 增加一组新的handler到当前组中,且不依赖任何其他的组和handler, 用于disruptor中消费者第一组构建*/
    public EventHandlerGroup<T> addNew(final EventProcessorFactory<T>... processorFactories){
        return disruptor.addCreateEventProcessors(this, new Sequence[0], processorFactories);
    }
    /** 增加一组新的handler到当前组中,且不依赖任何其他的组和handler, 用于disruptor中消费者第一组构建*/
    public EventHandlerGroup<T> addWorkNew(final WorkHandler<? super T>... workHandlers){
        return disruptor.addCreateWorkerPool(this, new Sequence[0], workHandlers);
    }

    /** 增加一组新的handler到当前组中,会依赖当前组的上一组中的handler */
    public EventHandlerGroup<T> add(final EventHandler<? super T>... eventHandlers){
        return disruptor.addCreateEventProcessors(this, dependencySequences, eventHandlers);
    }
    /** 增加一组新的handler到当前组中,会依赖当前组的上一组中的handler */
    public EventHandlerGroup<T> add(final EventProcessorFactory<T>... processorFactories){
        return disruptor.addCreateEventProcessors(this, dependencySequences, processorFactories);
    }
    /** 增加一组新的handler到当前组中,会依赖当前组的上一组中的handler */
    public EventHandlerGroup<T> addWork(final WorkHandler<? super T>... workHandlers){
        return disruptor.addCreateWorkerPool(this, dependencySequences, workHandlers);
    }

    private void convert(){
        if (this.nextDependencySequence == null || nextDependencySequence.length != nextDependencySequenceList.size()) {
            this.nextDependencySequence = this.nextDependencySequenceList.toArray(new Sequence[nextDependencySequenceList.size()]);
        }
    }


    /**
     * Create a new event handler group that combines the consumers in this group with <tt>otherHandlerGroup</tt>.
     *
     * @param otherHandlerGroup the event handler group to combine.
     * @return a new EventHandlerGroup combining the existing and new consumers into a single dependency group.
     */
    public EventHandlerGroup<T> and(final EventHandlerGroup<T> otherHandlerGroup)
    {
        final Sequence[] combinedSequences = new Sequence[this.dependencySequences.length + otherHandlerGroup.dependencySequences.length];
        System.arraycopy(this.dependencySequences, 0, combinedSequences, 0, this.dependencySequences.length);
        System.arraycopy(
            otherHandlerGroup.dependencySequences, 0,
            combinedSequences, this.dependencySequences.length, otherHandlerGroup.dependencySequences.length);
        return new EventHandlerGroup<T>(disruptor, consumerRepository, combinedSequences);
    }

    /**
     * Create a new event handler group that combines the handlers in this group with <tt>processors</tt>.
     *
     * @param processors the processors to combine.
     * @return a new EventHandlerGroup combining the existing and new processors into a single dependency group.
     */
    public EventHandlerGroup<T> and(final EventProcessor... processors)
    {
        Sequence[] combinedSequences = new Sequence[dependencySequences.length + processors.length];

        for (int i = 0; i < processors.length; i++)
        {
            consumerRepository.add(processors[i]);
            combinedSequences[i] = processors[i].getSequence();
        }
        System.arraycopy(dependencySequences, 0, combinedSequences, processors.length, dependencySequences.length);

        return new EventHandlerGroup<T>(disruptor, consumerRepository, combinedSequences);
    }

    /**
     * Set up batch handlers to consume events from the ring buffer. These handlers will only process events
     * after every {@link EventProcessor} in this group has processed the event.
     * <p>
     * <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     * <p>
     * <pre><code>dw.handleEventsWith(A).then(B);</code></pre>
     *
     * @param handlers the batch handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     */
    public EventHandlerGroup<T> then(final EventHandler<? super T>... handlers)
    {
        return handleEventsWith(handlers);
    }

    /**
     * <p>Set up custom event processors to handle events from the ring buffer. The Disruptor will
     * automatically start these processors when {@link Disruptor#start()} is called.</p>
     * <p>
     * <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     *
     * @param eventProcessorFactories the event processor factories to use to create the event processors that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     */
    public EventHandlerGroup<T> then(final EventProcessorFactory<T>... eventProcessorFactories)
    {
        return handleEventsWith(eventProcessorFactories);
    }

    /**
     * Set up a worker pool to handle events from the ring buffer. The worker pool will only process events
     * after every {@link EventProcessor} in this group has processed the event. Each event will be processed
     * by one of the work handler instances.
     * <p>
     * <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     * process events before the worker pool with handlers <code>B, C</code>:</p>
     * <p>
     * <pre><code>dw.handleEventsWith(A).thenHandleEventsWithWorkerPool(B, C);</code></pre>
     *
     * @param handlers the work handlers that will process events. Each work handler instance will provide an extra thread in the worker pool.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     */
    public EventHandlerGroup<T> thenHandleEventsWithWorkerPool(final WorkHandler<? super T>... handlers)
    {
        return handleEventsWithWorkerPool(handlers);
    }

    /**
     * Set up batch handlers to handle events from the ring buffer. These handlers will only process events
     * after every {@link EventProcessor} in this group has processed the event.
     * <p>
     * <p>This method is generally used as part of a chain. For example if <code>A</code> must
     * process events before <code>B</code>:</p>
     * <p>
     * <pre><code>dw.after(A).handleEventsWith(B);</code></pre>
     *
     * @param handlers the batch handlers that will process events.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     */
    public EventHandlerGroup<T> handleEventsWith(final EventHandler<? super T>... handlers)
    {
        convert(); // 将list转换为数组
        return disruptor.createEventProcessors(nextDependencySequence, handlers);
    }

    /**
     * <p>Set up custom event processors to handle events from the ring buffer. The Disruptor will
     * automatically start these processors when {@link Disruptor#start()} is called.</p>
     * <p>
     * <p>This method is generally used as part of a chain. For example if <code>A</code> must
     * process events before <code>B</code>:</p>
     * <p>
     * <pre><code>dw.after(A).handleEventsWith(B);</code></pre>
     *
     * @param eventProcessorFactories the event processor factories to use to create the event processors that will process events.
     * @return a {@link EventHandlerGroup} that can be used to chain dependencies.
     */
    public EventHandlerGroup<T> handleEventsWith(final EventProcessorFactory<T>... eventProcessorFactories)
    {
        convert(); // 将list转换为数组
        return disruptor.createEventProcessors(nextDependencySequence, eventProcessorFactories);
    }

    /**
     * Set up a worker pool to handle events from the ring buffer. The worker pool will only process events
     * after every {@link EventProcessor} in this group has processed the event. Each event will be processed
     * by one of the work handler instances.
     * <p>
     * <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     * process events before the worker pool with handlers <code>B, C</code>:</p>
     * <p>
     * <pre><code>dw.after(A).handleEventsWithWorkerPool(B, C);</code></pre>
     *
     * @param handlers the work handlers that will process events. Each work handler instance will provide an extra thread in the worker pool.
     * @return a {@link EventHandlerGroup} that can be used to set up a event processor barrier over the created event processors.
     */
    public EventHandlerGroup<T> handleEventsWithWorkerPool(final WorkHandler<? super T>... handlers)
    {
        convert(); // 将list转换为数组
        return disruptor.createWorkerPool(nextDependencySequence, handlers);
    }

    /**
     * Create a dependency barrier for the processors in this group.
     * This allows custom event processors to have dependencies on
     * {@link com.lmax.disruptor.BatchEventProcessor}s created by the disruptor.
     *
     * @return a {@link SequenceBarrier} including all the processors in this group.
     */
    public SequenceBarrier asSequenceBarrier()
    {
        convert(); // 将list转换为数组
        return disruptor.getRingBuffer().newBarrier(nextDependencySequence);
    }
}
