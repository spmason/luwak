package uk.co.flax.luwak.directory;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;

/**
 * A memory-resident {@link IndexOutput} implementation which uses a {@link ByteBufferFile} as storage
 */
public class ByteBufferOutputStream extends IndexOutput {

    private final ByteBufferFile file;
    private final Checksum crc = new BufferedChecksum(new CRC32());
    private int position;

    protected ByteBufferOutputStream(String name, ByteBufferFile file) {
        super("ByteBufferOutputStream(name=" + name + ")", name);
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
    public long getChecksum() throws IOException {
        return crc.getValue();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        crc.update(b);
        file.writeByte(position++, b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        crc.update(b, offset, length);
        file.write(position, b, offset, length);
        position += length;
    }
}
