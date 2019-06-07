package com.hazelcast.jet.impl.serialization;

import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.lang.invoke.SerializedLambda;

public class SerializedLambdaHook implements SerializerHook<SerializedLambda> {
    @Override
    public Class<SerializedLambda> getSerializationType() {
        return SerializedLambda.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<SerializedLambda>() {
            @Override
            public int getTypeId() {
                return SerializerHookConstants.SERIALIZED_LAMBDA;
            }

            @Override
            public void destroy() {

            }

            @Override
            public void write(ObjectDataOutput out, SerializedLambda object) throws IOException {
                out.writeUTF(object.getCapturingClass());
                out.writeUTF(object.getFunctionalInterfaceClass());
                out.writeUTF(object.getFunctionalInterfaceMethodName());
                out.writeUTF(object.getFunctionalInterfaceMethodSignature());
                out.writeUTF(object.getImplClass());
                out.writeUTF(object.getImplMethodName());
                out.writeUTF(object.getImplMethodSignature());
                out.writeInt(object.getImplMethodKind());
                out.writeUTF(object.getInstantiatedMethodType());

                int capturedArgCount = object.getCapturedArgCount();
                out.writeInt(capturedArgCount);
                for (int i = 0; i < capturedArgCount; i++) {
                    out.writeObject(object.getCapturedArg(i));
                }
            }

            @Override
            public SerializedLambda read(ObjectDataInput in) throws IOException {
                String capturingClassString = in.readUTF().replace('/', '.');
                ClassLoader classLoader = in.getClassLoader();
                Class<?> capturingClass;
                try {
                    capturingClass = ClassLoaderUtil.loadClass(classLoader, capturingClassString);
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
                String functionalInterfaceClass = in.readUTF();
                String functionalInterfaceMethodName = in.readUTF();
                String functionalInterfaceMethodSignature = in.readUTF();
                String implClass = in.readUTF();
                String implMethodName = in.readUTF();
                String implMethodSignature = in.readUTF();
                int implMethodKind = in.readInt();
                String instantiatedMethodType = in.readUTF();
                int capturedArgCount = in.readInt();
                Object[] capturedArgs = new Object[capturedArgCount];
                for (int i = 0; i < capturedArgCount; i++) {
                    capturedArgs[i] = in.readObject();
                }

                return new SerializedLambda(capturingClass, functionalInterfaceClass, functionalInterfaceMethodName,
                        functionalInterfaceMethodSignature, implMethodKind, implClass, implMethodName,
                        implMethodSignature, instantiatedMethodType, capturedArgs);
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return false;
    }
}
