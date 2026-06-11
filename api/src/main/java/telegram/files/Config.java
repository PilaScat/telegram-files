package telegram.files;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.log.dialect.jdk.JdkLog;
import com.openai.models.ChatModel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import java.util.logging.*;

public class Config {
    public static final String LOG_LEVEL = StrUtil.blankToDefault(System.getenv("LOG_LEVEL"), "INFO");

    public static final String APP_ENV = StrUtil.blankToDefault(System.getenv("APP_ENV"), "prod");

    public static final String APP_ROOT = System.getenv("APP_ROOT");

    public static final String DB_TYPE = StrUtil.blankToDefault(System.getenv("DB_TYPE"), "sqlite");

    public static final String DB_HOST = System.getenv("DB_HOST");

    public static final int DB_PORT = Convert.toInt(System.getenv("DB_PORT"), 0);

    public static final String DB_USER = System.getenv("DB_USER");

    public static final String DB_PASSWORD = System.getenv("DB_PASSWORD");

    public static final String DB_NAME = System.getenv("DB_NAME");

    public static final boolean DB_NEED_CREATE = Convert.toBool(System.getenv("DB_NEED_CREATE"), false);

    public static final String LOG_PATH = APP_ROOT + File.separator + "logs";

    public static final String TELEGRAM_ROOT = APP_ROOT + File.separator + "account";

    public static final int TELEGRAM_API_ID = Convert.toInt(System.getenv("TELEGRAM_API_ID"), 0);

    public static final String TELEGRAM_API_HASH = System.getenv("TELEGRAM_API_HASH");

    public static final int TELEGRAM_LOG_LEVEL = Convert.toInt(System.getenv("TELEGRAM_LOG_LEVEL"), 0);

    public static final String OPENAI_MODEL = StrUtil.blankToDefault(System.getenv("OPENAI_MODEL"), ChatModel.GPT_4O_MINI.asString());

    public static final DeploymentOptions VIRTUAL_THREAD_DEPLOYMENT_OPTIONS = new DeploymentOptions()
            .setThreadingModel(ThreadingModel.VIRTUAL_THREAD);

    private static Level logLevel;

    static {
        if (APP_ENV == null) {
            throw new RuntimeException("APP_ENV is not set");
        }
        if (APP_ROOT == null) {
            throw new RuntimeException("APP_ROOT is not set");
        }
        if (TELEGRAM_API_ID == 0) {
            throw new RuntimeException("TELEGRAM_API_ID is not set");
        }
        if (TELEGRAM_API_HASH == null) {
            throw new RuntimeException("TELEGRAM_API_HASH is not set");
        }

        if (!FileUtil.exist(APP_ROOT)) {
            FileUtil.mkdir(APP_ROOT);
        }
        if (!FileUtil.exist(TELEGRAM_ROOT)) {
            FileUtil.mkdir(TELEGRAM_ROOT);
        }
        if (!FileUtil.exist(LOG_PATH)) {
            FileUtil.mkdir(LOG_PATH);
        }

        initLogger();
    }

    public static boolean isProd() {
        return "prod".equals(APP_ENV);
    }

    public static void initLogger() {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.INFO);

        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$s] %5$s %6$s%n");

        if (ArrayUtil.isNotEmpty(rootLogger.getHandlers())) {
            for (Handler handler : rootLogger.getHandlers()) {
                rootLogger.removeHandler(handler);
            }
        }

        IgnoreExceptionLogFilter connectionErrorFilter = new IgnoreExceptionLogFilter();

        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINEST);
        consoleHandler.setFormatter(new SimpleFormatter());
        consoleHandler.setFilter(connectionErrorFilter);
        rootLogger.addHandler(consoleHandler);

        try {
            String logFilePattern = LOG_PATH + File.separator + "api.log";

            FileHandler fileHandler = new FileHandler(logFilePattern, 5000000, 3, true);
            fileHandler.setLevel(Level.FINEST);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setFilter(connectionErrorFilter);
            rootLogger.addHandler(fileHandler);
        } catch (IOException e) {
            System.out.println("Failed to create log FileHandler: " + e.getMessage());
        }

        try {
            logLevel = Level.parse(Config.LOG_LEVEL);
            System.out.println("Setting telegram.files log level to " + logLevel);
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid log level [" + Config.LOG_LEVEL + "], using default INFO.");
        }

        Logger nettyLogger = Logger.getLogger("io.netty");
        nettyLogger.setLevel(Level.WARNING);

        Logger.getLogger("telegram.files").setLevel(logLevel);
    }

    public static boolean isSqlite() {
        return Objects.equals(DB_TYPE, "sqlite");
    }

    public static boolean isPostgres() {
        return Objects.equals(DB_TYPE, "postgres");
    }

    public static boolean isMysql() {
        return Objects.equals(DB_TYPE, "mysql");
    }

    /**
     * Drops benign client-disconnect exceptions (broken pipe / connection reset / closed channel)
     * that Vert.x logs at SEVERE when a client aborts an in-flight file download (e.g. seeking a
     * video or closing the tab). These are normal and would otherwise flood the logs with stack traces.
     */
    public static class IgnoreExceptionLogFilter implements Filter {
        @Override
        public boolean isLoggable(LogRecord record) {
            Throwable t = record.getThrown();
            if (t instanceof ClosedChannelException) {
                return false;
            }
            if (t instanceof IOException && t.getMessage() != null) {
                String message = t.getMessage().toLowerCase();
                return !(message.contains("broken pipe")
                         || message.contains("connection reset")
                         || message.contains("connection was aborted")
                         || message.contains("forcibly closed"));
            }
            return true;
        }
    }

    public static class JDKLogFactory extends LogFactory {

        public JDKLogFactory() {
            super("JDK Logging");
        }

        public Log createLog(String name) {
            return new JdkLog(name);
        }

        public Log createLog(Class<?> clazz) {
            return new JdkLog(clazz);
        }
    }
}
