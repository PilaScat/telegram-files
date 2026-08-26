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
import java.util.Optional;

/**
 * Deletes cache copies of media that has already been transferred away, and drops them from
 * TDLib's persistent download list so they are not fetched again.
 */
public class OrphanCleanupMaintainVerticle extends MaintainVerticle {

    private final boolean apply;

    private volatile int scanned = 0;

    private volatile int orphans = 0;

    private volatile int removed = 0;

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

            log.info("✅ Scanned %d transferred records, %d had a cache copy, %d removed, %s freed. Time consumed: %s"
                    .formatted(scanned, orphans, removed, FileUtil.readableFileSize(freedBytes), timeInterval.intervalPretty()));
            if (!apply && orphans > 0) {
                log.info("✅ Dry run: nothing was deleted. Re-run with --apply to remove them.");
            }
            super.end(true, null);
        } catch (Exception e) {
            log.error("🔨 Failed to clean up orphan cache files", e);
            super.end(false, e);
        }
    }

    private void handleRecord(FileRecord fileRecord) {
        Optional<TelegramVerticle> telegramVerticleOptional = TelegramVerticles.get(fileRecord.telegramId());
        if (telegramVerticleOptional.isEmpty()) {
            return;
        }
        TelegramVerticle telegramVerticle = telegramVerticleOptional.get();
        try {
            TdApi.File file = Future.await(telegramVerticle.client.execute(new TdApi.GetFile(fileRecord.id())));
            String cachePath = file == null || file.local == null ? null : file.local.path;
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
            log.error(e, "🔨 Failed to clean up file unique id: %s".formatted(fileRecord.uniqueId()));
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
