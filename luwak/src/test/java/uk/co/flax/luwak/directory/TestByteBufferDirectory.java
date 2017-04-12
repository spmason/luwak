package uk.co.flax.luwak.directory;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

public class TestByteBufferDirectory {

    @Test
    public void testSingleBytes() throws IOException, InterruptedException {
        ByteBufferDirectory directory = new ByteBufferDirectory();

        try (IndexOutput output = directory.createOutput("test", new IOContext())) {
            output.writeByte((byte) 1);
            assertThat(output.getFilePointer()).isEqualTo(1);
        }
        assertThat(directory.fileLength("test")).isEqualTo(1);

        try (IndexInput input = directory.openInput("test", new IOContext())) {
            assertThat(input.readByte()).isEqualTo((byte) 1);
        }
    }

    @Test
    public void testBulkBytes() throws IOException, InterruptedException {
        final byte[] BYTES = new byte[] { 1, 2, 3, 4};

        ByteBufferDirectory directory = new ByteBufferDirectory();

        try (IndexOutput output = directory.createOutput("test", new IOContext())) {
            output.writeBytes(BYTES, BYTES.length);
            assertThat(output.getFilePointer()).isEqualTo(BYTES.length);
        }
        assertThat(directory.fileLength("test")).isEqualTo(BYTES.length);

        try (IndexInput input = directory.openInput("test", new IOContext())) {
            byte[] b = new byte[BYTES.length];
            input.readBytes(b, 0, BYTES.length);

            assertThat(b).isEqualTo(BYTES);
        }
    }

    @Test
    public void testMultipleWrites() throws IOException, InterruptedException {
        final byte[] BYTES = new byte[] { 1, 2, 3, 4};
        final int ITERATIONS = 2;

        ByteBufferDirectory directory = new ByteBufferDirectory();

        try (IndexOutput output = directory.createOutput("test", new IOContext())) {
            for (int i = 0; i < ITERATIONS; i++) {
                output.writeBytes(BYTES, BYTES.length);
                assertThat(output.getFilePointer()).isEqualTo(BYTES.length * (i+1));
            }
        }
        assertThat(directory.fileLength("test")).isEqualTo(8);

        try (IndexInput input = directory.openInput("test", new IOContext())) {
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] b = new byte[BYTES.length];
                input.readBytes(b, 0, BYTES.length);

                assertThat(b).isEqualTo(BYTES);
            }
        }
    }

    @Test
    public void testGrowingSize() throws IOException {
        final byte[] BYTES = new byte[] { 1, 2, 3, 4 };
        final int ITERATIONS = 512;

        ByteBufferDirectory directory = new ByteBufferDirectory();

        try (IndexOutput output = directory.createOutput("test", new IOContext())) {
            for (int i = 0; i < ITERATIONS; i++) {
                output.writeBytes(BYTES, BYTES.length);
                assertThat(output.getFilePointer()).isEqualTo(BYTES.length * (i + 1));
            }
        }
        assertThat(directory.fileLength("test")).isEqualTo(ITERATIONS * BYTES.length);

        try (IndexInput input = directory.openInput("test", new IOContext())) {
            for (int i = 0; i < ITERATIONS; i++) {
                byte[] b = new byte[BYTES.length];
                input.readBytes(b, 0, BYTES.length);

                assertThat(b).isEqualTo(BYTES);
                assertThat(input.getFilePointer()).isEqualTo(BYTES.length * (i + 1));
            }
        }
    }

    @Test
    public void testGrowingSize_moreThanBuffer() throws IOException {
        final byte[] BYTES = new byte[2049];

        ByteBufferDirectory directory = new ByteBufferDirectory();

        try (IndexOutput output = directory.createOutput("test", new IOContext())) {
            output.writeBytes(BYTES, BYTES.length);
            assertThat(output.getFilePointer()).isEqualTo(BYTES.length);
        }
        assertThat(directory.fileLength("test")).isEqualTo(2049);

        try (IndexInput input = directory.openInput("test", new IOContext())) {
            byte[] b = new byte[BYTES.length];
            input.readBytes(b, 0, BYTES.length);

            assertThat(b).isEqualTo(BYTES);
            assertThat(input.getFilePointer()).isEqualTo(2049);
        }
    }

    @Test
    public void copyBytes() throws IOException {
        final byte[] BYTES = new byte[] { 1, 2, 3, 4};

        ByteBufferDirectory directory = new ByteBufferDirectory();

        try (IndexOutput output = directory.createOutput("test", new IOContext())) {
            output.writeBytes(BYTES, BYTES.length);
        }

        try (IndexInput input = directory.openInput("test", new IOContext())) {
            try (IndexOutput output = directory.createOutput("test2", new IOContext())) {
                output.copyBytes(input, BYTES.length);
            }
        }

        try (IndexInput input = directory.openInput("test2", new IOContext())) {
            byte[] b = new byte[BYTES.length];
            input.readBytes(b, 0, BYTES.length);

            assertThat(b).isEqualTo(BYTES);
        }
    }

    @Test
    public void slice() throws IOException {
        final byte[] BYTES = new byte[] { 1, 2, 3, 4};

        ByteBufferDirectory directory = new ByteBufferDirectory();

        try (IndexOutput output = directory.createOutput("test", new IOContext())) {
            output.writeBytes(BYTES, BYTES.length);
        }

        try (IndexInput input = directory.openInput("test", new IOContext())) {
            try (IndexInput slicedInput = input.slice("foo", 1, 2)) {
                assertThat(slicedInput.getFilePointer()).isEqualTo(0);
                assertThat(slicedInput.length()).isEqualTo(2);

                assertThat(slicedInput.readByte()).isEqualTo((byte) 2);
                assertThat(slicedInput.readByte()).isEqualTo((byte) 3);
            }
        }
    }
}