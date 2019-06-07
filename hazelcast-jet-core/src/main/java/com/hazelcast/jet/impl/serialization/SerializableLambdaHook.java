package com.hazelcast.jet.impl.serialization;

import com.hazelcast.jet.function.SerializableLambda;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SerializableLambdaHook implements SerializerHook<SerializableLambda> {
    @Override
    public Class<SerializableLambda> getSerializationType() {
        return SerializableLambda.class;
    }

    @Override
    public Serializer createSerializer() {
        return new SerializableLambdaStreamSerializer();
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }

    private static class SerializableLambdaStreamSerializer implements StreamSerializer<SerializableLambda> {
        private static final Method READ_RESOLVE_METHOD = getReadResolve();

        private static Method getReadResolve() {
            try {
                Method readResolve = SerializedLambda.class.getDeclaredMethod("readResolve");
                readResolve.setAccessible(true);
                return readResolve;
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public int getTypeId() {
            return SerializerHookConstants.SERIALIZABLE_LAMBDA;
        }

        @Override
        public void destroy() {

        }

        @Override
        public void write(ObjectDataOutput out, SerializableLambda object) throws IOException {
            try {
                Method writeReplace = object.getClass().getDeclaredMethod("writeReplace");
                writeReplace.setAccessible(true);
                SerializedLambda serializableLambda = (SerializedLambda) writeReplace.invoke(object);

                out.writeObject(serializableLambda);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        @Override
        public SerializableLambda read(ObjectDataInput in) throws IOException {
            SerializedLambda sl = in.readObject();
            try {
                return (SerializableLambda) READ_RESOLVE_METHOD.invoke(sl);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }
    }
}
