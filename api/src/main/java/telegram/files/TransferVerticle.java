package telegram.files;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.jooq.lambda.tuple.Tuple3;
import telegram.files.repository.FileRecord;
import telegram.files.repository.SettingAutoRecords;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TransferVerticle extends AbstractVerticle {
    private static final Log log = LogFactory.get();

    private static final int HISTORY_SCAN_INTERVAL = 2 * 60 * 1000;

    private static final int TRANSFER_INTERVAL = 3 * 1000;

    private final SettingAutoRecords autoRecords;

    private final Map<String, Transfer> transfers = new HashMap<>();

    private final BlockingQueue<WaitingTransferFile> waitingTransferFiles = new LinkedBlockingQueue<>();

    // uniqueIds seen as 'completed' on the event bus, waiting to be resolved by the periodic
    // drain. The event handler itself must never touch the DB (see initEventConsumer).
    private final Set<String> pendingCompletedFiles = ConcurrentHashMap.newKeySet();

    private static final int MAX_PENDING_DRAIN_PER_TICK = 100;

    private static final long STALL_WARN_INTERVAL = 10 * 60 * 1000;

    private volatile boolean isStopped = false;

    private volatile Transfer beingTransferred;

    private volatile Future<Void> currentTransferFuture;

    private volatile String beingTransferredUniqueId;

    private volatile long beingTransferredSince;

    private long lastStallWarnTime;

    public TransferVerticle() {
        this.autoRecords = AutomationsHolder.INSTANCE.autoRecords();
        AutomationsHolder.INSTANCE.registerOnRemoveListener(removedItems -> removedItems.forEach(item -> {
            waitingTransferFiles.removeIf(waitingTransferFile -> waitingTransferFile.uniqueId().equals(item.uniqueKey()));
            transfers.remove(item.uniqueKey());
        }));
    }

    @Override
    public void start(Promise<Void> startPromise) {
        initEventConsumer().onSuccess(_ -> {
            vertx.setPeriodic(0, HISTORY_SCAN_INTERVAL, _ -> addHistoryFiles());
            vertx.setPeriodic(0, TRANSFER_INTERVAL, _ -> {
                drainPendingCompletedFiles();
                warnIfTransferStalled();
                startTransfer();
            });

            log.info("""
                    Transfer verticle started!
                    |History scan interval: %s ms
                    |Transfer interval: %s ms
                    |Auto chats: %s
                    """.formatted(HISTORY_SCAN_INTERVAL, TRANSFER_INTERVAL, autoRecords.getTransferEnabledItems().size()));

            startPromise.complete();
        }).onFailure(startPromise::fail);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        isStopped = true;
        Future<Void> inFlight = currentTransferFuture;
        if (inFlight == null || inFlight.isComplete()) {
            log.info("Transfer verticle stopped");
            stopPromise.complete();
            return;
        }
        // Don't spin-block the context waiting (the transfer's onComplete runs on this very
        // context): compose on the in-flight future, with a bounded grace period.
        log.info("Wait for transfer to complete, file: %s".formatted(beingTransferredUniqueId));
        long timerId = vertx.setTimer(30000, _ -> stopPromise.tryComplete());
        inFlight.onComplete(_ -> {
            vertx.cancelTimer(timerId);
            log.info("Transfer verticle stopped");
            stopPromise.tryComplete();
        });
    }

    private Future<Void> initEventConsumer() {
        vertx.eventBus().consumer(EventEnum.TELEGRAM_EVENT.address(), message -> {
            JsonObject jsonObject = (JsonObject) message.body();
            EventPayload payload = jsonObject.getJsonObject("payload").mapTo(EventPayload.class);
            if (payload == null || payload.type() != EventPayload.TYPE_FILE_STATUS) {
                return;
            }

            // This handler must stay non-blocking: this verticle's context is serialized, so a
            // handler that awaits the DB gets the consumer paused under load and events are
            // discarded ("Discarding message ... in paused consumer"). Just note the uniqueId;
            // the periodic drain resolves it.
            if (payload.data() instanceof Map<?, ?> data
                && "completed".equals(data.get("downloadStatus"))
                && data.get("uniqueId") instanceof String uniqueId) {
                pendingCompletedFiles.add(uniqueId);
            }
        });

        return Future.succeededFuture();
    }

    /**
     * Resolves the uniqueIds collected by the event consumer (bounded batch per tick) and moves
     * the transferable ones into the waiting queue. Runs on the verticle context, so DB awaits
     * are fine here.
     */
    private void drainPendingCompletedFiles() {
        Iterator<String> iterator = pendingCompletedFiles.iterator();
        int drained = 0;
        while (iterator.hasNext() && drained < MAX_PENDING_DRAIN_PER_TICK) {
            String uniqueId = iterator.next();
            iterator.remove();
            drained++;

            FileRecord fileRecord = Future.await(DataVerticle.fileRepository.getByUniqueId(uniqueId));
            if (fileRecord == null || "thumbnail".equals(fileRecord.type())) {
                // Thumbnails are internal preview files; never transfer them.
                continue;
            }

            SettingAutoRecords.Automation automation = null;
            if (fileRecord.threadChatId() != 0 && fileRecord.messageThreadId() != 0 && fileRecord.threadChatId() == fileRecord.chatId()) {
                // thread message file,try to get the main message
                FileRecord mainFileRecord = Future.await(DataVerticle.fileRepository.getMainFileByThread(
                        fileRecord.telegramId(),
                        fileRecord.threadChatId(),
                        fileRecord.messageThreadId()));
                if (mainFileRecord != null) {
                    automation = autoRecords.getItem(mainFileRecord.telegramId(), mainFileRecord.chatId());
                }
            } else {
                automation = autoRecords.getItem(fileRecord.telegramId(), fileRecord.chatId());
            }

            if (automation == null || !automation.transfer.enabled || getTransfer(automation) == null) {
                continue;
            }

            if (addWaitingTransferFile(automation.telegramId, automation.chatId, fileRecord.uniqueId())) {
                log.debug("Add file to transfer queue: %s".formatted(fileRecord.uniqueId()));
            }
        }
    }

    private void warnIfTransferStalled() {
        if (beingTransferred == null || beingTransferredSince == 0) {
            return;
        }
        long elapsed = System.currentTimeMillis() - beingTransferredSince;
        if (elapsed > STALL_WARN_INTERVAL && System.currentTimeMillis() - lastStallWarnTime > STALL_WARN_INTERVAL) {
            log.warn("Transfer of %s has been running for %d minutes, the transfer pipeline is stalled behind it"
                    .formatted(beingTransferredUniqueId, elapsed / 60000));
            lastStallWarnTime = System.currentTimeMillis();
        }
    }

    private void addHistoryFiles() {
        if (CollUtil.isEmpty(autoRecords.automations)) {
            return;
        }
        log.trace("Start scan history files for transfer");
        for (SettingAutoRecords.Automation automation : autoRecords.automations) {
            if (!automation.transfer.enabled
                || !automation.transfer.rule.transferHistory
                || automation.isComplete(SettingAutoRecords.HISTORY_TRANSFER_STATE)) {
                continue;
            }
            Transfer transfer = getTransfer(automation);
            if (transfer == null) {
                continue;
            }
            Tuple3<List<FileRecord>, Long, Long> filesTuple = Future.await(DataVerticle.fileRepository.getFiles(automation.chatId,
                    Map.of("downloadStatus", FileRecord.DownloadStatus.completed.name(),
                            "transferStatus", FileRecord.TransferStatus.idle.name()
                    )
            ));
            List<FileRecord> files = filesTuple.v1;
            if (CollUtil.isEmpty(files)) {
                log.debug("No history files found for transfer: %s".formatted(automation.uniqueKey()));
                automation.complete(SettingAutoRecords.HISTORY_TRANSFER_STATE);
                continue;
            }

            int count = 0;
            for (FileRecord fileRecord : files) {
                if ("thumbnail".equals(fileRecord.type())) {
                    // Thumbnails are internal preview files; never transfer them.
                    continue;
                }
                if (addWaitingTransferFile(fileRecord)) {
                    count++;
                }
            }

            if (count > 0) {
                log.info("Add history files to transfer queue: %s".formatted(count));
                break;
            }
        }
    }

    private boolean addWaitingTransferFile(FileRecord fileRecord) {
        return addWaitingTransferFile(fileRecord.telegramId(), fileRecord.chatId(), fileRecord.uniqueId());
    }

    private boolean addWaitingTransferFile(long telegramId, long chatId, String uniqueId) {
        WaitingTransferFile waitingTransferFile = new WaitingTransferFile(telegramId, chatId, uniqueId);
        if (!waitingTransferFiles.contains(waitingTransferFile)) {
            waitingTransferFiles.add(waitingTransferFile);
            return true;
        }
        return false;
    }

    private Transfer getTransfer(SettingAutoRecords.Automation automation) {
        if (automation == null || !automation.transfer.enabled) {
            return null;
        }

        SettingAutoRecords.TransferRule transferRule = automation.transfer.rule;

        if (transfers.containsKey(automation.uniqueKey())) {
            Transfer transfer = transfers.get(automation.uniqueKey());
            if (!transfer.isRuleUpdated(transferRule)) {
                return transfer;
            } else {
                log.debug("Transfer rule updated: %s".formatted(automation.uniqueKey()));
                transfers.remove(automation.uniqueKey());
            }
        }

        return transfers.computeIfAbsent(automation.uniqueKey(), _ -> {
            Transfer transfer = Transfer.create(transferRule);
            transfer.transferStatusUpdated = updated ->
                    updateTransferStatus(updated.fileRecord(), updated.transferStatus(), updated.localPath());
            return transfer;
        });
    }

    public void startTransfer() {
        if (beingTransferred != null) {
            return;
        }
        try {
            WaitingTransferFile waitingTransferFile = waitingTransferFiles.poll();
            if (waitingTransferFile == null) {
                log.trace("No file to transfer");
                return;
            }
            Transfer transfer = transfers.get("%d:%d".formatted(waitingTransferFile.telegramId(), waitingTransferFile.chatId()));
            if (transfer == null) {
                // The item was already polled — resolve the automation lazily instead of
                // silently dropping the file.
                transfer = getTransfer(autoRecords.getItem(waitingTransferFile.telegramId(), waitingTransferFile.chatId()));
                if (transfer == null) {
                    log.debug("No transfer automation for %s, dropping %s"
                            .formatted(waitingTransferFile.chatId(), waitingTransferFile.uniqueId()));
                    return;
                }
            }
            FileRecord fileRecord = Future.await(DataVerticle.fileRepository.getByUniqueId(waitingTransferFile.uniqueId));
            if (fileRecord == null) {
                log.error("File not found: %s".formatted(waitingTransferFile.uniqueId));
                return;
            }

            startTransfer(fileRecord, transfer);
        } catch (Exception e) {
            log.error(e, "Transfer error");
        }
    }

    public void startTransfer(FileRecord fileRecord, Transfer transfer) {
        if (isStopped) {
            return;
        }
        if (!fileRecord.isDownloadStatus(FileRecord.DownloadStatus.completed)
            || StrUtil.isBlank(fileRecord.localPath())) {
            log.warn("File {} is not downloaded yet", fileRecord.id());
            return;
        }
        if (fileRecord.transferStatus() != null
            && !fileRecord.isTransferStatus(FileRecord.TransferStatus.idle)) {
            log.debug("File {} transfer status is not idle: {}", fileRecord.id(), fileRecord.transferStatus());
            return;
        }

        beingTransferred = transfer;
        beingTransferredUniqueId = fileRecord.uniqueId();
        beingTransferredSince = System.currentTimeMillis();
        // Run the transfer (MD5 compares + cross-device move — slow, blocking file I/O) on the
        // worker pool: doing it on this serialized context froze the event consumer and the
        // timers for the whole duration. beingTransferred keeps the pipeline serial, and is
        // reset in onComplete so a crashed transfer can't freeze the pipeline forever.
        currentTransferFuture = vertx.executeBlocking(() -> {
            transfer.transfer(fileRecord);
            return (Void) null;
        }, false);
        currentTransferFuture.onComplete(r -> {
            beingTransferred = null;
            beingTransferredUniqueId = null;
            beingTransferredSince = 0;
            if (r.failed()) {
                log.error(r.cause(), "Transfer error");
            }
        });
    }

    private void updateTransferStatus(FileRecord fileRecord, FileRecord.TransferStatus transferStatus, String localPath) {
        // NB: runs on a worker thread (called from Transfer.transfer inside executeBlocking), so
        // Future.await (which needs a virtual-thread context) can't be used here.
        MessyUtils.await(DataVerticle.fileRepository.updateTransferStatus(fileRecord.uniqueId(), transferStatus, localPath)
                .onSuccess(fileUpdated -> {
                    if (fileUpdated != null && !fileUpdated.isEmpty()) {
                        EventPayload payload = EventPayload.build(EventPayload.TYPE_FILE_STATUS, new JsonObject()
                                .put("fileId", fileRecord.id())
                                .put("uniqueId", fileRecord.uniqueId())
                                .put("transferStatus", fileUpdated.getString("transferStatus"))
                                .put("localPath", fileUpdated.getString("localPath"))
                        );
                        vertx.eventBus().publish(EventEnum.TELEGRAM_EVENT.address(),
                                JsonObject.of("telegramId", fileRecord.telegramId(), "payload", JsonObject.mapFrom(payload))
                        );
                    }
                }));
    }

    private record WaitingTransferFile(long telegramId, long chatId, String uniqueId) {
    }
}
