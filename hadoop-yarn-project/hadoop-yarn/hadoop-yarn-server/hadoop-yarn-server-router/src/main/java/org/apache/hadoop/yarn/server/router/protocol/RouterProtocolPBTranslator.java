package org.apache.hadoop.yarn.server.router.protocol;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Helper class for setting/getting data elements in an object backed by a
 * protobuf implementation.
 */
public class RouterProtocolPBTranslator<P extends GeneratedMessage,
    B extends Message.Builder, T extends MessageOrBuilder> {

  /** Optional proto byte stream used to create this object. */
  private P proto;
  /** The class of the proto handler for this translator. */
  private Class<P> protoClass;
  /** Internal builder, used to store data that has been set. */
  private B builder;

  public RouterProtocolPBTranslator(Class<P> protoType) {
    this.protoClass = protoType;
  }

  /**
   * Called if this translator is to be created from an existing protobuf byte
   * stream.
   *
   * @param p The existing proto object to use to initialize the translator.
   * @throws IllegalArgumentException
   */
  @SuppressWarnings("unchecked")
  public void setProto(Message p) {
    if (protoClass.isInstance(p)) {
      if (this.builder != null) {
        // Merge with builder
        this.builder.mergeFrom((P) p);
      } else {
        // Store proto
        this.proto = (P) p;
      }
    } else {
      throw new IllegalArgumentException(
          "Cannot decode proto type " + p.getClass().getName());
    }
  }

  /**
   * Create or return the cached protobuf builder for this translator.
   *
   * @return cached Builder instance
   */
  @SuppressWarnings("unchecked")
  public B getBuilder() {
    if (this.builder == null) {
      try {
        Method method = protoClass.getMethod("newBuilder");
        this.builder = (B) method.invoke(null);
        if (this.proto != null) {
          // Merge in existing immutable proto
          this.builder.mergeFrom(this.proto);
        }
      } catch (ReflectiveOperationException e) {
        this.builder = null;
      }
    }
    return this.builder;
  }

  /**
   * Get the serialized proto object. If the translator was created from a byte
   * stream, returns the intitial byte stream. Otherwise creates a new byte
   * stream from the cached builder.
   *
   * @return Protobuf message object
   */
  @SuppressWarnings("unchecked")
  public P build() {
    if (this.builder != null) {
      // serialize from builder (mutable) first
      Message m = this.builder.build();
      return (P) m;
    } else if (this.proto != null) {
      // Use immutable message source, message is unchanged
      return this.proto;
    }
    return null;
  }

  /**
   * Returns an interface to access data stored within this object. The object
   * may have been initialized either via a builder or by an existing protobuf
   * byte stream.
   *
   * @return MessageOrBuilder protobuf interface for the requested class.
   */
  @SuppressWarnings("unchecked")
  public T getProtoOrBuilder() {
    if (this.builder != null) {
      // Use mutable builder if it exists
      return (T) this.builder;
    } else if (this.proto != null) {
      // Use immutable message source
      return (T) this.proto;
    } else {
      // Construct empty builder
      return (T) this.getBuilder();
    }
  }

  /**
   * Read instance from base64 data.
   * @param base64String
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void readInstance(String base64String) throws IOException {
    byte[] bytes = Base64.decodeBase64(base64String);
    Message msg = getBuilder().mergeFrom(bytes).build();
    this.proto = (P) msg;
  }
}
