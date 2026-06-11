package telegram.files.repository;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.vertx.core.json.JsonObject;
import telegram.files.MessyUtils;
import telegram.files.Transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SettingAutoRecords {
    public List<Automation> automations;

    public static final int HISTORY_PRELOAD_STATE = 1;

    public static final int HISTORY_DOWNLOAD_STATE = 2;

    public static final int HISTORY_DOWNLOAD_SCAN_STATE = 3;

    public static final int HISTORY_TRANSFER_STATE = 4;

    public static class Automation {
        public long telegramId;

        public long chatId;

        public PreloadConfig preload;

        public DownloadConfig download;

        public TransferConfig transfer;

        public int state;

        public String uniqueKey() {
            return telegramId + ":" + chatId;
        }

        @JsonIgnore
        public void complete(int bitwise) {
            MessyUtils.BitState bitState = new MessyUtils.BitState(state);
            bitState.enableState(bitwise);
            state = bitState.getState();
        }

        @JsonIgnore
        public boolean isComplete(int bitwise) {
            MessyUtils.BitState bitState = new MessyUtils.BitState(state);
            return bitState.isStateEnabled(bitwise);
        }

        @JsonIgnore
        public boolean isNotComplete(int bitwise) {
            return !isComplete(bitwise);
        }
    }

    public static class PreloadConfig {
        public boolean enabled;

        public long nextFromMessageId;

        public PreloadConfig with(PreloadConfig config) {
            this.enabled = config.enabled;
            return this;
        }
    }

    public static class DownloadConfig {
        public boolean enabled;

        public DownloadRule rule;

        public String nextFileType;

        public long nextFromMessageId;

        public DownloadConfig with(DownloadConfig config) {
            this.enabled = config.enabled;
            this.rule = config.rule;
            return this;
        }
    }

    public static class DownloadRule {
        public String query;

        public List<String> fileTypes;

        public boolean downloadHistory;

        public boolean downloadCommentFiles;

        public String filterExpr;
    }

    public static class TransferConfig {
        public boolean enabled;

        public TransferRule rule;

        public TransferConfig with(TransferConfig config) {
            this.enabled = config.enabled;
            this.rule = config.rule;
            return this;
        }
    }

    public static class TransferRule {
        public boolean transferHistory;

        public String destination;

        public Transfer.TransferPolicy transferPolicy;

        public Transfer.DuplicationPolicy duplicationPolicy;

        // When true, the post caption is appended to the destination file name.
        public boolean useCaptionName;

        public JsonObject extra;
    }

    public SettingAutoRecords() {
        this.automations = new ArrayList<>();
    }

    public SettingAutoRecords(List<Automation> automations) {
        this.automations = automations;
    }

    public boolean exists(long telegramId, long chatId) {
        return automations.stream().anyMatch(item -> item.telegramId == telegramId && item.chatId == chatId);
    }

    public void add(Automation item) {
        automations.removeIf(i -> i.telegramId == item.telegramId && i.chatId == item.chatId);
        automations.add(item);
    }

    public void remove(long telegramId, long chatId) {
        automations.removeIf(item -> item.telegramId == telegramId && item.chatId == chatId);
    }

    @JsonIgnore
    public List<Automation> getPreloadEnabledItems() {
        return automations.stream()
                .filter(i -> i.preload != null && i.preload.enabled)
                .toList();
    }

    @JsonIgnore
    public List<Automation> getDownloadEnabledItems() {
        return automations.stream()
                .filter(i -> i.download != null && i.download.enabled)
                .toList();
    }

    @JsonIgnore
    public List<Automation> getTransferEnabledItems() {
        return automations.stream()
                .filter(i -> i.transfer != null && i.transfer.enabled)
                .toList();
    }

    public Map<Long, Automation> getItems(long telegramId) {
        return automations.stream()
                .filter(item -> item.telegramId == telegramId)
                .collect(Collectors.toMap(i -> i.chatId, Function.identity()));
    }

    public Automation getItem(long telegramId, long chatId) {
        return automations.stream()
                .filter(item -> item.telegramId == telegramId && item.chatId == chatId)
                .findFirst()
                .orElse(null);
    }

}
