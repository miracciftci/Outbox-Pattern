-- =====================================================================
--  SİPARİŞ SERVİSİ (üretici taraf)
--  orders + order_items  : iş verisi
--  outbox                : yayınlanacak event'ler  (iş verisiyle AYNI transaction'da yazılır)
--  shedlock              : relay'in çoklu instance'ta tek yerde çalışması için kilit tablosu
-- =====================================================================

CREATE TABLE orders (
    id            VARCHAR(36)              PRIMARY KEY,
    customer_id   VARCHAR(50)              NOT NULL,
    total_amount  DECIMAL(19, 2)           NOT NULL,
    currency      VARCHAR(3)               NOT NULL,
    status        VARCHAR(20)              NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE order_items (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id    VARCHAR(36)    NOT NULL REFERENCES orders (id),
    product_id  VARCHAR(50)    NOT NULL,
    quantity    INT            NOT NULL,
    unit_price  DECIMAL(19, 2) NOT NULL
);

CREATE TABLE outbox (
    id              VARCHAR(36)              PRIMARY KEY,  -- event id: consumer'da deduplication anahtarı
    aggregate_type  VARCHAR(100)             NOT NULL,     -- topic seçimi: "Order" -> order-events
    aggregate_id    VARCHAR(100)             NOT NULL,     -- Kafka key: aynı sipariş için sıra garantisi
    event_type      VARCHAR(100)             NOT NULL,     -- OrderCreated, OrderCancelled ...
    payload         CLOB                     NOT NULL,     -- event'in yazma anındaki JSON hali (snapshot)
    status          VARCHAR(20)              NOT NULL,     -- PENDING / PROCESSED / FAILED
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at    TIMESTAMP WITH TIME ZONE,              -- relay yayınladığında doldurulur
    retry_count     INT DEFAULT 0            NOT NULL,
    last_error      VARCHAR(1000)
);

CREATE INDEX ix_outbox_pending ON outbox (status, created_at);

CREATE TABLE shedlock (
    name        VARCHAR(64)  NOT NULL PRIMARY KEY,
    lock_until  TIMESTAMP(3) NOT NULL,
    locked_at   TIMESTAMP(3) NOT NULL,
    locked_by   VARCHAR(255) NOT NULL
);

-- =====================================================================
--  FATURA SERVİSİ (tüketici taraf)
--  Gerçekte ayrı bir servis ve ayrı bir veritabanıdır; demo kolaylığı için
--  aynı uygulamada duruyor. Sipariş tablolarına hiç dokunmaz.
--  inbox    : işlenmiş event id'leri (idempotent consumer)
--  invoices : iş verisi
-- =====================================================================

CREATE TABLE inbox (
    event_id      VARCHAR(36)              PRIMARY KEY,  -- PK çakışması duplicate'i engeller
    event_type    VARCHAR(100)             NOT NULL,
    processed_at  TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE invoices (
    id           VARCHAR(36)              PRIMARY KEY,
    order_id     VARCHAR(36)              NOT NULL,
    customer_id  VARCHAR(50)              NOT NULL,
    amount       DECIMAL(19, 2)           NOT NULL,
    currency     VARCHAR(3)               NOT NULL,
    event_id     VARCHAR(36)              NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL
);
