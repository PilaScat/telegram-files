package telegram.files.maintains;

import cn.hutool.core.collection.IterUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.templates.SqlTemplate;
import org.drinkless.tdlib.TdApi;
import telegram.files.Config;
import telegram.files.DataVerticle;
import telegram.files.TelegramVerticle;
import telegram.files.TelegramVerticles;
import telegram.files.repository.FileRecord;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Deletes cache copies of media that has already been transferred away, and drops them from
 * TDLib's persistent download list so they are not fetched again.
 */
public class OrphanCleanupMaintainVerticle extends MaintainVerticle {

    private static final long AUTHORIZATION_TIMEOUT = 120 * 1000;

    private static final long AUTHORIZATION_POLL_INTERVAL = 500;

    private final boolean apply;

    private volatile int scanned = 0;

    private volatile int orphans = 0;

    private volatile int removed = 0;

    private volatile int failed = 0;

    private volatile long freedBytes = 0;

    public OrphanCleanupMaintainVerticle(boolean apply) {
        this.apply = apply;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        super.start(startPromise, this::cleanup);
    }

    private void cleanup() {
        timeInterval.start();
        log.info("🔨 Start to clean up orphan cache files%s".formatted(apply ? "" : " (dry run)"));
        try {
            if (!awaitAuthorization()) {
                log.error("""
                        🔨 No telegram account became authorized within %d seconds, so no file could be checked.
                        🔨 TDLib only answers GetFile once its own initialization has completed, and its database
                        🔨 cannot be opened twice: stop the running telegram-files container and run this again."""
                        .formatted(AUTHORIZATION_TIMEOUT / 1000));
                super.end(false, new IllegalStateException("No authorized telegram account"));
                return;
            }

            long fromMessageId = 0;
            while (true) {
                List<FileRecord> rows = Future.await(SqlTemplate.forQuery(DataVerticle.pool, """
                                SELECT * FROM file_record
                                WHERE transfer_status = 'completed' AND type != 'thumbnail'
                                %s
                                ORDER BY message_id desc LIMIT 100
                                """.formatted(fromMessageId == 0 ? "" : " AND message_id < #{fromMessageId}")
                        )
                        .mapTo(FileRecord.ROW_MAPPER)
                        .execute(MapUtil.of("fromMessageId", fromMessageId))
                        .map(IterUtil::toList));

                if (rows.isEmpty()) {
                    break;
                }

                for (FileRecord fileRecord : rows) {
                    scanned++;
                    handleRecord(fileRecord);
                }

                fromMessageId = rows.getLast().messageId();
            }

            report();
            super.end(failed < scanned, null);
        } catch (Exception e) {
            log.error("🔨 Failed to clean up orphan cache files", e);
            super.end(false, e);
        }
    }

    private void report() {
        int checked = scanned - failed;
        if (failed > 0) {
            log.error("🔨 %d of %d transferred records could not be checked against TDLib, and were left alone."
                    .formatted(failed, scanned));
        }
        if (checked == 0) {
            log.error("🔨 No record could be checked, so nothing is known about cache copies. Time consumed: %s"
                    .formatted(timeInterval.intervalPretty()));
            return;
        }
        log.info("✅ Checked %d of %d transferred records, %d had a cache copy, %d removed, %s freed. Time consumed: %s"
                .formatted(checked, scanned, orphans, removed, FileUtil.readableFileSize(freedBytes), timeInterval.intervalPretty()));
        if (!apply && orphans > 0) {
            log.info("✅ Dry run: nothing was deleted. Re-run with --apply to remove them.");
        }
    }

    private boolean awaitAuthorization() {
        long deadline = System.currentTimeMillis() + AUTHORIZATION_TIMEOUT;
        while (System.currentTimeMillis() < deadline) {
            if (TelegramVerticles.getAll().stream().anyMatch(t -> t.authorized)) {
                return true;
            }
            try {
                Thread.sleep(AUTHORIZATION_POLL_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private void handleRecord(FileRecord fileRecord) {
        Optional<TelegramVerticle> telegramVerticleOptional = TelegramVerticles.get(fileRecord.telegramId());
        if (telegramVerticleOptional.isEmpty() || !telegramVerticleOptional.get().authorized) {
            failed++;
            return;
        }
        TelegramVerticle telegramVerticle = telegramVerticleOptional.get();
        try {
            TdApi.File file = Future.await(telegramVerticle.client.execute(new TdApi.GetFile(fileRecord.id())));
            if (file == null || file.remote == null || !Objects.equals(file.remote.uniqueId, fileRecord.uniqueId())) {
                log.warn("🔨 TDLib file id %d no longer refers to %s, skipping"
                        .formatted(fileRecord.id(), fileRecord.uniqueId()));
                failed++;
                return;
            }

            String cachePath = file.local == null ? null : file.local.path;
            if (!isCacheCopy(cachePath)) {
                return;
            }

            orphans++;
            long size = FileUtil.size(FileUtil.file(cachePath));
            log.info("🔨 %s cache copy of transferred file %s: %s (%s)"
                    .formatted(apply ? "Removing" : "Would remove",
                            fileRecord.uniqueId(),
                            cachePath,
                            FileUtil.readableFileSize(size)));
            if (!apply) {
                return;
            }

            Future.await(telegramVerticle.client.execute(new TdApi.RemoveFileFromDownloads(fileRecord.id(), true), true));
            if (FileUtil.exist(cachePath)) {
                FileUtil.del(cachePath);
            }
            removed++;
            freedBytes += size;
        } catch (Exception e) {
            failed++;
            log.error(e, "🔨 Failed to check file unique id: %s".formatted(fileRecord.uniqueId()));
        }
    }

    private boolean isCacheCopy(String cachePath) {
        if (StrUtil.isBlank(cachePath) || !FileUtil.exist(cachePath)) {
            return false;
        }
        Path path = Path.of(cachePath).toAbsolutePath().normalize();
        return path.startsWith(Path.of(Config.TELEGRAM_ROOT).toAbsolutePath().normalize());
    }
}
