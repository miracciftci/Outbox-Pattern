# Outbox Pattern — Spring Boot Sipariş Örneği

Bir e-ticaret sitesinde **sipariş oluşturulduğunda** `OrderCreated` event'inin Kafka'ya
**güvenilir şekilde** yayınlanmasını gösteren demo proje.

```
 Sipariş Servisi            Tek DB, tek transaction                Relay              Kafka              Fatura Servisi
 POST /api/orders  ──►  INSERT orders + order_items  ─┐                                               
                        INSERT outbox (PENDING)      ─┘ COMMIT                                          
                                                        ◄── oku (FOR UPDATE SKIP LOCKED)               
                                                             publish key=orderId ──►  order-events  ──► inbox'ta var mı?
                                                        ◄── ack, PROCESSED                            yoksa: fatura + inbox
```

Proje, [Outbox Pattern.pdf](Outbox%20Pattern.pdf) sunumuyla birlikte kullanılmak üzere hazırlandı. Sunumdaki
kod örnekleri (OrderService, OutboxRelay, OrderEventsListener, application.yml) bu projeden alınmıştır.

## Çalıştırma

Gereken tek şey **Java 17+**. Kafka kurmaya gerek yok: uygulama, içinde gömülü bir Kafka broker başlatır.

```bash
./mvnw spring-boot:run        # Windows: mvnw.cmd spring-boot:run
```

Ardından tarayıcıda **http://localhost:8080** adresini açın. Demo panelinde butonlarla tüm
senaryoları çalıştırıp tabloların canlı değişimini izleyebilirsiniz.

| Adres | Ne var |
|---|---|
| http://localhost:8080 | Demo paneli |
| http://localhost:8080/h2-console | Veritabanı (JDBC URL: `jdbc:h2:mem:orderdb`, kullanıcı `sa`, şifre boş) |
| http://localhost:8080/actuator/metrics/outbox.pending.count | Outbox metriği |
| [requests.http](requests.http) | VS Code REST Client / IntelliJ için hazır istekler |

> **H2 Console'a giriş:** Giriş ekranı JDBC URL alanını varsayılan olarak `jdbc:h2:~/test` diye doldurur.
> Bunu elle `jdbc:h2:mem:orderdb` yapmazsanız boş bir veritabanına bağlanırsınız. Veritabanı bellekte
> tutulduğu için uygulama her yeniden başladığında sıfırlanır.

> **VS Code:** Sınıf adlarına Ctrl+tıklama ile gitmek için *Extension Pack for Java* eklentisi gerekir.

Testler (tüm senaryolar otomatik doğrulanır):

```bash
./mvnw test
```

### Gerçek Kafka ile (isteğe bağlı)

```bash
docker compose up -d
./mvnw spring-boot:run -Dspring-boot.run.profiles=docker
```

Kafka UI: http://localhost:8090 (partition, key ve `eventId` header'ını görmek için).

> **Not — sertifika hatası (PKIX):** Şirket ağı veya antivirüs SSL'i araya giriyorsa Maven
> bağımlılık indirirken `PKIX path building failed` hatası verebilir. Windows'ta şu çözer:
> `set MAVEN_OPTS=-Djavax.net.ssl.trustStoreType=Windows-ROOT` (PowerShell: `$env:MAVEN_OPTS='...'`)

## Demo senaryoları

| # | Senaryo | Ne gösteriyor | Beklenen sonuç |
|---|---|---|---|
| 1 | **Sipariş oluştur** | Normal outbox akışı | Sipariş → outbox `PENDING` → `PROCESSED` → fatura |
| 2 | **Hatalı sipariş** | Commit öncesi hata | Sipariş de outbox kaydı da yok, Kafka'ya hiçbir şey gitmedi |
| 3 | **Naive + hata** | Dual write: **hayalet event** | Sipariş yok ama fatura kesilmiş (panelde kırmızı) |
| 4 | **Relay'i durdur** | Ayrık yayınlama | Siparişler alınıyor, event'ler outbox'ta birikiyor; devam edince sırayla gidiyor |
| 5 | **Broker'ı çökert** | Kullanılabilirlik | Outbox: sipariş alınır (201), relay retry yapar, 5 denemede `FAILED`. Naive: 503, sipariş alınamaz |
| 6 | **Gönderip çök** | At-least-once + inbox | Event Kafka'ya **iki kez** gider; consumer logunda `DUPLICATE yakalandı`, tek fatura kalır |

Konsol loglarını da açık tutun; her adım (`Yayınlandı ... -> order-events-2@5`, `Alındı ... partition=2`,
`DUPLICATE yakalandı`, `HAYALET EVENT!`) okunabilir şekilde loglanır.

## Kodda nereye bakmalı?

| Konu | Dosya |
|---|---|
| ❌ Problem: dual write | [NaiveOrderService.java](src/main/java/com/example/outbox/order/NaiveOrderService.java) |
| ✅ Çözüm: tek transaction | [OrderService.java](src/main/java/com/example/outbox/order/OrderService.java) |
| Outbox'a yazma (`Propagation.MANDATORY`) | [OutboxWriter.java](src/main/java/com/example/outbox/outbox/OutboxWriter.java) |
| Outbox tablosu | [schema.sql](src/main/resources/schema.sql), [OutboxEvent.java](src/main/java/com/example/outbox/outbox/OutboxEvent.java) |
| Polling Publisher (Relay) | [OutboxRelay.java](src/main/java/com/example/outbox/outbox/OutboxRelay.java) |
| `FOR UPDATE SKIP LOCKED` sorgusu | [OutboxRepository.java](src/main/java/com/example/outbox/outbox/OutboxRepository.java) |
| ShedLock (çoklu instance) | [ShedLockConfig.java](src/main/java/com/example/outbox/config/ShedLockConfig.java) |
| Producer / consumer ayarları | [application.yml](src/main/resources/application.yml) |
| Payload tasarımı (snapshot, `schemaVersion`) | [OrderCreatedEvent.java](src/main/java/com/example/outbox/order/OrderCreatedEvent.java) |
| Idempotent consumer + inbox | [OrderEventsListener.java](src/main/java/com/example/outbox/invoice/OrderEventsListener.java) |
| Dead Letter Topic | [KafkaConfig.java](src/main/java/com/example/outbox/config/KafkaConfig.java) |
| Temizlik job'ı | [OutboxCleanupJob.java](src/main/java/com/example/outbox/outbox/OutboxCleanupJob.java) |
| İzleme metrikleri | [OutboxMetrics.java](src/main/java/com/example/outbox/outbox/OutboxMetrics.java) |
| Senaryo testleri | [OutboxFlowIntegrationTest.java](src/test/java/com/example/outbox/OutboxFlowIntegrationTest.java) |

## Tasarım kararları

- **Kafka key = `orderId`.** Aynı siparişin event'leri aynı partition'a düşer, sırası korunur.
  Topic, `aggregate_type`'tan türetilir: `Order` → `order-events`.
- **Relay sırayı korur.** Bir event gönderilemezse o tur durur, sonraki event'ler sonraki turda denenir.
  `max-retries` aşılınca kayıt `FAILED` olur ki kuyruğu sonsuza kadar kilitlemesin (alarm + elle `retry`).
- **At-least-once.** Relay Kafka'ya gönderip kaydı `PROCESSED` yapamadan çökerse event tekrar gider.
  Exactly-once yoktur; duplicate'i consumer'daki **inbox** tablosu yakalar (iş verisiyle aynı transaction'da).
  Inbox her tür işlem için çalışan genel bir çözümdür. Alternatifi, işlemi doğal olarak idempotent
  tasarlamaktır: "stoğu 2 azalt" yerine "durumu ÖDENDİ yap" gibi değere getiren işlemler veya
  `UNIQUE (order_id)` + upsert ile her sipariş için tek fatura. E-posta göndermek ya da stok düşmek gibi
  işlemler doğal olarak idempotent olamayacağı için projede inbox kullanıldı.
- **Outbox temizliği.** `OutboxCleanupJob` dakikada bir, `outbox.cleanup.retention` süresinden (demoda 10 dk)
  eski **yalnızca `PROCESSED`** kayıtları siler. `PENDING` kayıtlar silinmez (mesaj kaybolur); `FAILED` kayıtlar
  da silinmez (incelenip elle `retry` edilmeleri gerekir). Kayıtların bir süre tutulması, "bu event ne zaman
  gitti?" sorusuna cevap verebilmek içindir.
- **Consumer offset'i DB commit'inden sonra ilerler** (`enable-auto-commit=false`, `ack-mode=record`).
- **Payload bir snapshot'tır.** Relay entity'yi yeniden okumaz; event yazıldığı andaki hâliyle gönderilir.
- **`OutboxWriter` transaction olmadan çağrılamaz** (`Propagation.MANDATORY`); yanlışlıkla dual write'a dönülmez.
- **Fatura Servisi aynı uygulamada** ama yalnızca kendi tablolarını (`invoices`, `inbox`) kullanır ve
  üreticinin sınıflarına bağımlı değildir. Gerçekte ayrı servis ve ayrı veritabanıdır.

## Gerçek projede farklı olacaklar

- `spring-kafka-test` ve `EmbeddedKafkaConfig` yalnızca demo içindir; gerçek projede test scope'ta olur.
- `demo` paketi (hata simülasyon anahtarları) gerçek kodda bulunmaz.
- **Retry stratejisi.** Demoda relay 1 saniye arayla 5 kez dener, sonra kaydı `FAILED` yapar. Yani ~5 saniyelik
  bir Kafka kesintisi bile o sıradaki event'leri `FAILED` yapar ve elle `retry` gerekir. Gerçek projede
  `next_attempt_at` kolonu ile artan bekleme (1 sn, 2 sn, 4 sn…) kullanılır; geçici hatalarda (bağlantı, timeout)
  kayıt `FAILED` yapılmaz, yalnızca kalıcı hatalarda (bozuk payload, çok büyük mesaj) `FAILED` + alarm üretilir.
- **Temizlik.** Demodaki temizlik tek bir `DELETE` ile çalışır. Milyonlarca satırda bu uzun bir transaction ve
  kilit demektir; gerçek projede silme parça parça yapılır (ör. her seferinde 1000 satır). Yüksek hacimde tablo
  tarih bazlı partition'lara bölünür ve eski partition tek komutla `DROP` edilir. Saklama süresi genelde
  birkaç gündür (`P3D`). Debezium (CDC) kullanılıyorsa kayıt eklendiği anda silinebilir.
- **Inbox temizliği yok.** `inbox` tablosu da zamanla büyür ama demoda onu temizleyen bir job yoktur. Eklenirse
  saklama süresi, bir mesajın tekrar gelebileceği en uzun süreden (pratikte Kafka retention süresi, ör. 7 gün)
  uzun olmalıdır; aksi hâlde silinen bir `eventId` tekrar gelirse duplicate olarak yakalanamaz.
- H2 yerine PostgreSQL/Oracle.
- Yüksek hacimde Polling yerine **Debezium Outbox Event Router** (CDC). Outbox tablosu aynı kalır.
- Event yazımı `ApplicationEventPublisher` + `@TransactionalEventListener(BEFORE_COMMIT)` ile soyutlanabilir.
