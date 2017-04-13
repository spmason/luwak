package uk.co.flax.luwak.directory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wraps a {@link ByteBuffer} to allow random access and dynamic growth / reallocation
 */
public class ByteBufferFile implements Cloneable {
    private static final int BUFFER_SIZE = 1024;

    private ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final AtomicLong length = new AtomicLong();
    private final Lock positionReadLock;
    private final Lock positionWriteLock;

    public ByteBufferFile() {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        positionReadLock = readWriteLock.readLock();
        positionWriteLock = readWriteLock.writeLock();
    }

    private ByteBufferFile(byte[] bytes) {
        this();
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
        lockPosition(position, () -> buffer.get(b, offset, len));
    }

    public ByteBufferFile slice(int offset, int length) {
        byte[] bytes = new byte[length];
        lockPosition(offset, () -> buffer.get(bytes, 0, length));

        return new ByteBufferFile(bytes);
    }

    public void writeByte(int position, byte b) {
        if (position == buffer.capacity()) {
            increaseBufferSize(position, 1);
        }
        buffer.put(position, b);
        if (this.length.get() < position + 1) {
            this.length.set(position + 1);
        }
    }

    public void write(int position, byte[] b, int offset, int length) {
        if (buffer.capacity() < position + length) {
            increaseBufferSize(position, length);
        }
        lockPosition(position, () -> buffer.put(b, offset, length));
        this.length.set(position + length);
    }

    private void lockPosition(int position, Runnable then) {
        if (buffer.position() == position) {
            // Acquire lock then check again
            positionReadLock.lock();

            if (buffer.position() != position) {
                // something else changed the position - release our lock & try again
                positionReadLock.unlock();
                lockPosition(position, then);
                return;
            }
            try {
                then.run();
            } finally {
                positionReadLock.unlock();
            }
        } else {
            positionWriteLock.lock();
            try {
                buffer.position(position);
                then.run();
            } finally {
                positionWriteLock.unlock();
            }
        }
    }

    private void increaseBufferSize(int position, int desiredLengthIncrease) {
        int newLength = position + desiredLengthIncrease;
        int newCapacity = buffer.capacity();

        while (newCapacity < newLength) {
            newCapacity += BUFFER_SIZE;
        }

        ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCapacity);

        buffer.position(0);
        newBuffer.put(buffer);

        buffer = newBuffer;
    }
}
