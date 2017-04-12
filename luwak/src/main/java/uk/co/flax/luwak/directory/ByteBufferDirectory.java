package uk.co.flax.luwak.directory;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;

/**
 * A memory-resident {@link Directory} implementation.  Locking
 * implementation is by default the {@link SingleInstanceLockFactory}.
 */
public class ByteBufferDirectory extends BaseDirectory {
    private final Map<String, ByteBufferFile> fileMap = new ConcurrentHashMap<>();

    /** Used to generate temp file names in {@link #createTempOutput}. */
    private final AtomicLong nextTempFileCounter = new AtomicLong();

    /** Constructs an empty {@link ByteBufferDirectory}. */
    public ByteBufferDirectory() {
        super(new SingleInstanceLockFactory());
    }

    @Override
    public final String[] listAll() {
        ensureOpen();
        // NOTE: this returns a "weakly consistent view". Unless we change Dir API, keep this,
        // and do not synchronize or anything stronger. it's great for testing!
        // NOTE: fileMap.keySet().toArray(new String[0]) is broken in non Sun JDKs,
        // and the code below is resilient to map changes during the array population.
        // NOTE: don't replace this with return names.toArray(new String[names.size()]);
        // or some files could be null at the end of the array if files are being deleted
        // concurrently
        Set<String> fileNames = fileMap.keySet();
        List<String> names = new ArrayList<>(fileNames.size());
        names.addAll(fileNames);
        String[] namesArray = names.toArray(new String[names.size()]);
        Arrays.sort(namesArray);
        return namesArray;
    }

    /** Returns the length in bytes of a file in the directory.
     * @throws IOException if the file does not exist
     */
    @Override
    public final long fileLength(String name) throws IOException {
        ensureOpen();
        ByteBufferFile file = fileMap.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
        return file.getLength();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        ByteBufferFile file = fileMap.remove(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        ByteBufferFile file = newByteBufferFile();
        if (fileMap.putIfAbsent(name, file) != null) {
            throw new FileAlreadyExistsException(name);
        }
        return new ByteBufferOutputStream(name, file);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();

        // Make the file first...
        ByteBufferFile file = newByteBufferFile();

        // ... then try to find a unique name for it:
        while (true) {
            String name = IndexFileNames.segmentFileName(prefix, suffix + "_" + Long.toString(nextTempFileCounter.getAndIncrement(), Character.MAX_RADIX), "tmp");
            if (fileMap.putIfAbsent(name, file) == null) {
                return new ByteBufferOutputStream(name, file);
            }
        }
    }

    /**
     * Returns a new {@link ByteBufferFile} for storing data
     */
    private ByteBufferFile newByteBufferFile() {
        return new ByteBufferFile();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        ByteBufferFile file = fileMap.get(source);
        if (file == null) {
            throw new FileNotFoundException(source);
        }
        if (fileMap.putIfAbsent(dest, file) != null) {
            throw new FileAlreadyExistsException(dest);
        }
        if (!fileMap.remove(source, file)) {
            throw new IllegalStateException("file was unexpectedly replaced: " + source);
        }
        fileMap.remove(source);
    }

    @Override
    public void syncMetaData() throws IOException {
        // we are by definition not durable!
    }

    /** Returns a stream reading an existing file. */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        ByteBufferFile file = fileMap.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
        return new ByteBufferInputStream(name, file);
    }

    /** Closes the store to future operations, releasing associated memory. */
    @Override
    public void close() {
        isOpen = false;
        fileMap.clear();
    }
}
