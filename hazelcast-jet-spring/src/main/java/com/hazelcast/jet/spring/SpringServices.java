/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.spring;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.spring.context.SpringAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public final class SpringServices {

    private SpringServices() {

    }

    public static <I, O, T> FunctionEx<BatchStage<I>, BatchStage<O>> mapBatchUsingSpringBean(String beanName,
                                                                                             Class<T> requiredType,
                                                                                             BiFunctionEx<? super T, ? super I, ? extends O> mapper) {
        return e -> e.mapUsingService(beanServiceFactory(beanName, requiredType), mapper::apply);
    }

    public static <I, O, T> FunctionEx<BatchStage<I>, BatchStage<O>> mapBatchUsingSpringBean(Class<T> requiredType,
                                                                                             BiFunctionEx<? super T, ? super I, ? extends O> mapper) {
        return e -> e.mapUsingService(beanServiceFactory(requiredType), mapper::apply);
    }

    public static <I, O, T> FunctionEx<BatchStage<I>, BatchStage<O>> mapBatchUsingSpringBean(String beanName,
                                                                                             BiFunctionEx<? super T, ? super I, ? extends O> mapper) {
        return e -> e.mapUsingService(beanServiceFactory(beanName), mapper::apply);
    }

    public static <I, O, T> FunctionEx<StreamStage<I>, StreamStage<O>> mapStreamUsingSpringBean(String beanName,
                                                                                                Class<T> requiredType,
                                                                                                BiFunctionEx<? super T, ? super I, ? extends O> mapper) {
        return e -> e.mapUsingService(beanServiceFactory(beanName, requiredType), mapper::apply);
    }

    public static <I, O, T> FunctionEx<StreamStage<I>, StreamStage<O>> mapStreamUsingSpringBean(Class<T> requiredType,
                                                                                                BiFunctionEx<? super T, ? super I, ? extends O> mapper) {
        return e -> e.mapUsingService(beanServiceFactory(requiredType), mapper::apply);
    }

    public static <I, O, T> FunctionEx<StreamStage<I>, StreamStage<O>> mapStreamUsingSpringBean(String beanName,
                                                                                                BiFunctionEx<? super T, ? super I, ? extends O> mapper) {
        return e -> e.mapUsingService(beanServiceFactory(beanName), mapper::apply);
    }

    public static <I, T> FunctionEx<BatchStage<I>, BatchStage<I>> filterBatchUsingSpringBean(String beanName,
                                                                                                  Class<T> requiredType,
                                                                                                  BiPredicateEx<? super T, ? super I> predicate) {
        return e -> e.filterUsingService(beanServiceFactory(beanName, requiredType), predicate::test);
    }

    public static <I, T> FunctionEx<BatchStage<I>, BatchStage<I>> filterBatchUsingSpringBean(Class<T> requiredType,
                                                                                             BiPredicateEx<? super T, ? super I> predicate) {
        return e -> e.filterUsingService(beanServiceFactory(requiredType), predicate::test);
    }

    public static <I, T> FunctionEx<BatchStage<I>, BatchStage<I>> filterBatchUsingSpringBean(String beanName,
                                                                                             BiPredicateEx<? super T, ? super I> predicate) {
        return e -> e.filterUsingService(beanServiceFactory(beanName), predicate::test);
    }

    public static <I, T> FunctionEx<StreamStage<I>, StreamStage<I>> filterStreamUsingSpringBean(String beanName,
                                                                                             Class<T> requiredType,
                                                                                             BiPredicateEx<? super T, ? super I> predicate) {
        return e -> e.filterUsingService(beanServiceFactory(beanName, requiredType), predicate::test);
    }

    public static <I, T> FunctionEx<StreamStage<I>, StreamStage<I>> filterStreamUsingSpringBean(Class<T> requiredType,
                                                                                             BiPredicateEx<? super T, ? super I> predicate) {
        return e -> e.filterUsingService(beanServiceFactory(requiredType), predicate::test);
    }

    public static <I, T> FunctionEx<StreamStage<I>, StreamStage<I>> filterStreamUsingSpringBean(String beanName,
                                                                                             BiPredicateEx<? super T, ? super I> predicate) {
        return e -> e.filterUsingService(beanServiceFactory(beanName), predicate::test);
    }

    public static <T> ServiceFactory<?, T> beanServiceFactory(String beanName, Class<T> requiredType) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                .withCreateServiceFn((c, g) -> g.getBean(beanName, requiredType));
    }

    public static <T> ServiceFactory<?, T> beanServiceFactory(Class<T> requiredType) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                .withCreateServiceFn((c, g) -> g.getBean(requiredType));
    }

    public static <T> ServiceFactory<?, T> beanServiceFactory(String beanName) {
        return ServiceFactory.withCreateContextFn(ctx -> new BeanExtractor())
                .withCreateServiceFn((c, g) -> g.getBean(beanName));
    }

    @SpringAware
    private final static class BeanExtractor {
        @Autowired
        private transient ApplicationContext context;

        public <T> T getBean(String name) {
            return (T) context.getBean(name);
        }

        public <T> T getBean(String name, Class<T> type) {
            return context.getBean(name, type);
        }

        public <T> T getBean(Class<T> type) {
            return context.getBean(type);
        }
    }
}
