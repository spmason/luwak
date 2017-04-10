package uk.co.flax.luwak.directory;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * A memory-resident {@link IndexInput} implementation which uses a {@link ByteBufferFile} as storage
 */
public class ByteBufferInputStream extends IndexInput implements Cloneable {

    private final ByteBufferFile file;
    private int position;

    public ByteBufferInputStream(String name, ByteBufferFile file) {
        super("ByteBufferInputStream(name=" + name + ")");
        this.file = file;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pos cannot be longer than Integer.MAX_VALUE: " + pos);
        }
        position = (int) pos;
    }

    @Override
    public long length() {
        return file.getLength();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("offset cannot be longer than Integer.MAX_VALUE: " + offset);
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("length cannot be longer than Integer.MAX_VALUE: " + length);
        }
        return new ByteBufferInputStream(sliceDescription, file.slice((int) offset, (int) length));
    }

    @Override
    public byte readByte() throws IOException {
        return file.readByte(position++);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        file.read(position, b, offset, len);
        position += len;
    }
}
