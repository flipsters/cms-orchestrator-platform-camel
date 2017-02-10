package org.apache.camel.cms.orchestrator.aggregator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.camel.Exchange;
import org.apache.camel.NoTypeConversionAvailableException;
import org.apache.camel.TypeConversionException;
import org.apache.camel.TypeConverter;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by pawas.kumar on 02/02/17.
 */
public class CamelPayloadAggregatorTest {

    @Test
    public void testCamelPayloadAggregator() throws IOException, ClassNotFoundException {
        final ObjectMapper objectMapper = new ObjectMapper();
        MockTypeConverterRegistry mockTypeConverterRegistry = new MockTypeConverterRegistry();
        TypeConverter typeConverter = new TypeConverter() {
            @Override
            public boolean allowNull() {
                return false;
            }

            @Override
            public <T> T convertTo(Class<T> type, Object value) throws TypeConversionException {
                try {
                    return objectMapper.readValue((byte[]) value, type);
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e);
                }
            }

            @Override
            public <T> T convertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException {
                try {
                    return objectMapper.readValue((byte[]) value, type);
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e.getCause());
                }
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Object value) {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Exchange exchange, Object value) {
                return null;
            }
        };
        TypeConverter byteTypeConvertor = new TypeConverter() {
            @Override
            public boolean allowNull() {
                return false;
            }

            @Override
            public <T> T convertTo(Class<T> type, Object value) throws TypeConversionException {
                try {
                    return objectMapper.readValue((byte[]) value, new TypeReference<Payload<byte[]>>() {});
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e);
                }
            }

            @Override
            public <T> T convertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException {
                try {
                    return objectMapper.readValue((byte[]) value, type);
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e.getCause());
                }
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Object value) {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Exchange exchange, Object value) {
                return null;
            }
        };
        TypeConverter objectToByte = new TypeConverter() {
            @Override
            public boolean allowNull() {
                return false;
            }

            @Override
            public <T> T convertTo(Class<T> type, Object value) throws TypeConversionException {
                try {
                    return (T) objectMapper.writeValueAsBytes(value);
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e);
                }
            }

            @Override
            public <T> T convertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException {
                try {
                    return (T) objectMapper.writeValueAsBytes(value);
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e.getCause());
                }
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Object value) {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Exchange exchange, Object value) {
                return null;
            }
        };
        TypeConverter typeConverter1 = new TypeConverter() {
            @Override
            public boolean allowNull() {
                return false;
            }

            @Override
            public <T> T convertTo(Class<T> type, Object value) throws TypeConversionException {
                try {
                    return objectMapper.readValue((byte[]) value, new TypeReference<T>() {
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public <T> T convertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException {
                try {
                    return objectMapper.readValue((byte[]) value, new TypeReference<T>() {
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T mandatoryConvertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException, NoTypeConversionAvailableException {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Object value) {
                return null;
            }

            @Override
            public <T> T tryConvertTo(Class<T> type, Exchange exchange, Object value) {
                return null;
            }
        };
        Type type = new TypeReference<List<Class1>>() {
        }.getType();
        Type type1 = new TypeReference<ArrayList<Class1>>() {
        }.getType();
        mockTypeConverterRegistry.addTypeConverter(Payload.class, byte[].class, byteTypeConvertor);
        mockTypeConverterRegistry.addTypeConverter(Class1.class, byte[].class, typeConverter);
        mockTypeConverterRegistry.addTypeConverter(Class2.class, byte[].class, typeConverter);
        mockTypeConverterRegistry.addTypeConverter(Class3.class, byte[].class, typeConverter);
//        mockTypeConverterRegistry.addTypeConverter(new TypeReference<List<Class1>>() {}., Class2.class, objectToByte);
        mockTypeConverterRegistry.addTypeConverter(byte[].class, Class1.class, objectToByte);
        mockTypeConverterRegistry.addTypeConverter(byte[].class, Class2.class, objectToByte);
        mockTypeConverterRegistry.addTypeConverter(ArrayList.class, byte[].class, typeConverter1);
        mockTypeConverterRegistry.addTypeConverter(byte[].class, Payload.class, objectToByte);
        CamelPayloadAggregator<Class1, Class2> stringCamelPayloadAggregator = new CamelPayloadAggregator<>(new TestPayloadAggregator(), mockTypeConverterRegistry);
        Payload<Class1> hello = new Payload<>(new Class1(10, "hello"), null);
//        Payload<Class3> hello = new Payload<Class3>(new Class3(Lists.newArrayList(new Class1(10, "hello"))), null);
        Payload<byte[]> helloByte = new Payload<>(objectMapper.writeValueAsBytes(hello.getBody()), hello.getHeaders());
        Payload<Class2> world1 = new Payload<>(new Class2(20, "world"), null);
        Payload<byte[]> worldByte = new Payload<>(objectMapper.writeValueAsBytes(world1.getBody()), world1.getHeaders());
        byte[] hellos = objectMapper.writeValueAsBytes(helloByte);
        byte[] world = objectMapper.writeValueAsBytes(worldByte);
        byte[] aggregate = stringCamelPayloadAggregator.aggregate(world, hellos);
        Payload payload2 = getPayload(aggregate, mockTypeConverterRegistry, Class1.class);
        Assert.assertEquals("Payload(body=CamelPayloadAggregatorTest.Class1(super=org.apache.camel.cms.orchestrator.aggregator.CamelPayloadAggregatorTest$Class1@1bc47d03, hail=30, trump=worldhello), headers={})", payload2.toString());
    }

    public Payload getPayload(byte[] bytes, MockTypeConverterRegistry typeConverterRegistry, Class bodyType)
        throws IOException, ClassNotFoundException {
        Payload payload = typeConverterRegistry.lookup(Payload.class, byte[].class).convertTo(Payload.class, bytes);
        payload.setBody(typeConverterRegistry.lookup(bodyType, byte[].class).convertTo(bodyType, payload.getBody()));
        return payload;
    }

    class TestPayloadAggregator implements PayloadAggregator<Class1, Class2> {

        public Payload<Class2> aggregate(Payload<Class2> existing,  Payload<Class1> increment) throws IOException, ClassNotFoundException {
            Class2 body = existing.getBody();
            Class1 body1 = increment.getBody();

            existing.setBody(new Class2(body.getHail() + body1.getHail(), body.getTrump() + body1.getTrump()));
            return existing;
        }

        @Override
        public Class getExistingType() {
            return Class2.class;
        }

        @Override
        public Class getIncrementType() {
            return Class1.class;
        }

        @Override
        public String getId() {
            return "test";
        }
    }

    // @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
    interface Hail {

        int getHail();

        String getTrump();
    }

    @NoArgsConstructor
    @Data
    @ToString(callSuper = true)
    public static class Class1 implements Hail {

        public Class1(int hail, String trump) {
            this.hail = hail;
            this.trump = trump;
        }

        int hail;
        String trump;
    }

    @NoArgsConstructor
    @Data
    @ToString(callSuper = true)
    public static class Class2 implements Hail {

        public Class2(int hail, String trump) {
            this.hail = hail;
            this.trump = trump;
        }

        int hail;
        String trump;
    }

    @NoArgsConstructor
    @Data
    @ToString(callSuper = true)
    public static class Class3 {
        List<Class1> class1s;

        public Class3(List<Class1> class1s) {
            this.class1s = class1s;
        }
    }

    @Test
    public void test() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Class1 hail = new Class1(10, "donald");
        Class2 fail = new Class2(10, "trump");
        byte[] hellos = objectMapper.writeValueAsBytes(hail);
        System.out.println(new String(hellos));
        System.out.println(objectMapper.readValue(hellos, new TypeReference<Class1>() {}));
        System.out.println(objectMapper.readValue(hellos, Object.class));
    }
}