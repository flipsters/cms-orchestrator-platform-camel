/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.cms.orchestrator.aggregator;

import com.google.common.collect.Lists;
import org.apache.camel.TypeConverter;
import org.apache.camel.spi.Injector;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.util.ObjectHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MockTypeConverterRegistry implements TypeConverterRegistry {
    protected final ConcurrentMap<TypeMapping, TypeConverter> typeMappings = new ConcurrentHashMap<TypeMapping, TypeConverter>();
    private List<TypeConverter> fallbackTypeConverters = new ArrayList<TypeConverter>();
    
    public List<TypeConverter> getTypeConverters() {
        return Lists.newArrayList(typeMappings.values());
    }
    
    public List<TypeConverter> getFallbackTypeConverters() {
        return fallbackTypeConverters;
    }
    
    public void addTypeConverter(Class<?> toType, Class<?> fromType, TypeConverter typeConverter) {
        typeMappings.put(new TypeMapping(toType, fromType), typeConverter);
    }

    public boolean removeTypeConverter(Class<?> toType, Class<?> fromType) {
        // noop
        return true;
    }

    public void addFallbackTypeConverter(TypeConverter typeConverter, boolean canPromote) {
        fallbackTypeConverters.add(typeConverter);
    }

    public TypeConverter lookup(Class<?> toType, Class<?> fromType) {       
        return typeMappings.get(new TypeMapping(toType, fromType));
    }

    public List<Class<?>[]> listAllTypeConvertersFromTo() {
        return null;
    }

    public void setInjector(Injector injector) {
       // do nothing
    }

    public Injector getInjector() {
        return null;
    }

    public Statistics getStatistics() {
        return null;
    }

    public int size() {
        return getTypeConverters().size();
    }

    public void start() throws Exception {
        // noop
    }

    public void stop() throws Exception {
        // noop
    }

    protected static class TypeMapping {
        private final Class<?> toType;
        private final Class<?> fromType;

        TypeMapping(Class<?> toType, Class<?> fromType) {
            this.toType = toType;
            this.fromType = fromType;
        }

        public Class<?> getFromType() {
            return fromType;
        }

        public Class<?> getToType() {
            return toType;
        }

        @Override
        public boolean equals(Object object) {
            if (object instanceof TypeMapping) {
                TypeMapping that = (TypeMapping) object;
                return ObjectHelper.equal(this.fromType, that.fromType)
                    && ObjectHelper.equal(this.toType, that.toType);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int answer = toType.hashCode();
            if (fromType != null) {
                answer *= 37 + fromType.hashCode();
            }
            return answer;
        }

        @Override
        public String toString() {
            return "[" + fromType + "=>" + toType + "]";
        }

        public boolean isApplicable(Class<?> fromClass) {
            return fromType.isAssignableFrom(fromClass);
        }
    }
}

