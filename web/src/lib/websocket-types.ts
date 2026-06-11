export type WebSocketMessage = {
  type: number;
  code: string;
  data: unknown;
  timestamp: number;
};

export const WebSocketMessageType = {
  ERROR: -1,
  AUTHORIZATION: 1,
  METHOD_RESULT: 2,
  FILE_UPDATE: 3,
  FILE_DOWNLOAD: 4,
  FILE_STATUS: 5,
};

export type TelegramError = {
  code: string;
  message: string;
} & TelegramObject;

export type TelegramObject = {
  constructor: number;
} & Record<string, any>;

export const TelegramConstructor = {
  STATE_READY: -1834871737,
  WAIT_PHONE_NUMBER: 306402531,
  WAIT_CODE: 52643073,
  WAIT_PASSWORD: 112238030,
  WAIT_OTHER_DEVICE_CONFIRMATION: 860166378,
  WAIT_PREMIUM_PURCHASE: 2097616261,
  LOGGING_OUT: 154449270,
};
