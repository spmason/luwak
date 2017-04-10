package uk.co.flax.luwak.directory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps a {@link ByteBuffer} to allow random access and dynamic growth / reallocation
 */
public class ByteBufferFile implements Cloneable {
    private static final int BUFFER_SIZE = 1024;

    private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private AtomicLong length = new AtomicLong();

    public ByteBufferFile() {
    }

    private ByteBufferFile(byte[] bytes) {
        buffer = ByteBuffer.wrap(bytes);
        length.set(bytes.length);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(length=" + length + ")";
    }

    @Override
    public int hashCode() {
        long length = this.length.get();
        int h = (int) (length ^ (length >>> 32));
        return 31 * h + buffer.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ByteBufferFile other = (ByteBufferFile) obj;

        return length.get() == other.length.get() && buffer.equals(other.buffer);

    }

    public long getLength() {
        return length.get();
    }

    public byte readByte(int position) {
        return buffer.get(position);
    }

    public void read(int position, byte[] b, int offset, int len) {
        buffer.position(position);
        buffer.get(b, offset, len);
    }

    public ByteBufferFile slice(int offset, int length) {
        int oldPosition = buffer.position();
        byte[] bytes = new byte[length];
        buffer.position(offset);
        buffer.get(bytes, 0, length);
        buffer.position(oldPosition);

        return new ByteBufferFile(bytes);
    }

    public void writeByte(int position, byte b) {
        if (position == buffer.capacity()) {
            increaseBufferSize();
        }
        buffer.put(position, b);
        if (this.length.get() < position + 1) {
            this.length.set(position + 1);
        }
    }

    public void write(int position, byte[] b, int offset, int length) {
        if (buffer.capacity() < buffer.position() + length) {
            increaseBufferSize();
        }
        buffer.position(position);
        buffer.put(b, offset, length);
        this.length.set(buffer.position());
    }

    private synchronized void increaseBufferSize() {
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() + BUFFER_SIZE);

        buffer.position(0);
        newBuffer.put(buffer);

        buffer = newBuffer;
    }

    public void resetPosition() {
        this.buffer.position(0);
    }

    public long getPosition() {
        return this.buffer.position();
    }

    public void setPosition(int pos) {
        this.buffer.position(pos);
    }
}
