package telegram.files;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class AppInstanceLock {

    private static final String LOCK_FILE_NAME = ".app.lock";

    private static final int API_PORT = 8080;

    private static final int PROBE_TIMEOUT = 500;

    private static FileChannel channel;

    private static FileLock lock;

    private AppInstanceLock() {
    }

    public static synchronized void hold() {
        if (lock != null) {
            return;
        }
        try {
            channel = FileChannel.open(lockPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
            lock = channel.tryLock();
            if (lock == null) {
                closeChannel();
            }
        } catch (IOException | OverlappingFileLockException e) {
            closeChannel();
        }
    }

    public static boolean isApplicationRunning() {
        return isLockHeldByAnotherProcess() || isApiPortAnswering();
    }

    private static boolean isLockHeldByAnotherProcess() {
        try (FileChannel probe = FileChannel.open(lockPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            FileLock acquired = probe.tryLock();
            if (acquired == null) {
                return true;
            }
            acquired.release();
            return false;
        } catch (OverlappingFileLockException e) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean isApiPortAnswering() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", API_PORT), PROBE_TIMEOUT);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static Path lockPath() {
        return new File(Config.APP_ROOT, LOCK_FILE_NAME).toPath();
    }

    private static void closeChannel() {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ignored) {
        } finally {
            channel = null;
            lock = null;
        }
    }
}
