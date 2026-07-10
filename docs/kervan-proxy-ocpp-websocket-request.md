# Talep: kervan-proxy — OCPP WebSocket Reverse-Proxy Desteği

**Kime:** kervan-proxy geliştiricisi
**Kimden:** EaaS / GoOCPP ekibi
**Tarih:** 2026-07-07
**Konu:** kervan-proxy'nin bir OCPP CSMS'in (GoOCPP) önüne şeffaf WebSocket reverse-proxy olarak konabilmesi için gereken davranış değişiklikleri


---

## 1. Amaç

kervan-proxy'yi bir OCPP 1.6/2.0.1 CSMS'in (GoOCPP) **önüne** koymak ve şarj cihazlarının (CP) proxy üzerinden CSMS ile haberleşmesini sağlamak istiyoruz. Böylece proxy'nin dayanıklılık (upstream kesintisinde tamponlama/yeniden bağlanma) özelliğinden OCPP trafiğinde faydalanabileceğiz.

Hedeflenen topoloji:

```
Şarj Cihazı (CP) ──WebSocket (OCPP)──▶ kervan-proxy ──WebSocket (OCPP)──▶ CSMS (GoOCPP)
```

## 2. Mevcut durum (engel)

İnceleme sonucunda kervan-proxy'nin şu an bir **WebSocket → ham TCP köprüsü** olarak çalıştığını ve OCPP trafiğini bu haliyle geçiremediğini tespit ettik. Dört noktada uyumsuzluk var; ilk üçü bağlantının hiç kurulamamasına yol açıyor:

1. Gelen bağlantı tek ve sabit bir yola kilitli; OCPP'nin yol tabanlı adreslemesi (`/ocpp/{chargePointId}`) desteklenmiyor.
2. Upstream, ham TCP soketi olarak açılıyor; CSMS'e WebSocket HTTP Upgrade yapılmıyor.
3. WebSocket subprotocol (`ocpp1.6` / `ocpp2.0.1`) negotiate edilmiyor / iletilmiyor.
4. Client'a mesajlar binary frame olarak yazılıyor.

## 3. Talep edilen değişiklikler

Öncelik seviyeleri: **Zorunlu** (bu olmadan bağlantı hiç kurulamaz) · **Önerilen** (doğruluk/dayanıklılık ve doğru çok-kiracılı çalışma).

| # | Talep | Öncelik | Gerekçe |
|---|---|---|---|
| R1 | **Upstream'e gerçek bir WebSocket bağlantısı kurulması** (ham TCP yerine HTTP Upgrade ile `ws://` handshake). | **Zorunlu** | OCPP CSMS bir WebSocket endpoint'idir. Ham TCP byte'ları hiçbir zaman WebSocket'e upgrade olmaz; CSMS bağlantıyı geçersiz HTTP isteği olarak kapatır. Bu, en temel engeldir. |
| R2 | **Gelen isteğin yolunun (path) upstream'e olduğu gibi iletilmesi.** Proxy tek sabit endpoint'e kilitli olmamalı; `/ocpp/{chargePointId}` gibi yollar korunmalı. | **Zorunlu** | OCPP'de şarj cihazının kimliği bağlantı yolunda taşınır. Yol kaybolursa CSMS hangi CP'nin bağlandığını çözemez; ayrıca sabit yol nedeniyle istek 404 alır. |
| R3 | **WebSocket subprotocol negotiation'ının uçtan uca korunması.** Proxy, client'ın handshake'te istediği subprotocol'ü (`ocpp1.6` / `ocpp2.0.1`) hem client'a geri onaylamalı hem upstream handshake'ine taşımalı. | **Zorunlu** | CSMS, protokol sürümünü subprotocol'e göre seçer. Subprotocol boş gelirse GoOCPP bağlantıyı **"unsupported protocol"** diyerek reddeder (doğrulandı). |
| R4 | **OCPP mesajlarının WebSocket text frame olarak iletilmesi.** | **Önerilen** | OCPP-J spesifikasyonu mesajların UTF-8 JSON text frame ile taşınmasını şart koşar. Bazı uçlar (GoOCPP ve mevcut simülatör dahil) frame tipini yok sayıp yine de çalışır; ancak gerçek şarj cihazı firmware'leri ve katı CSMS'ler binary frame'i reddeder. Proxy'nin sadık/şeffaf bir OCPP proxy olması için text kullanılmalı. |
| R5 | **Tek bir OCPP mesajının bölünmeden, mesaj sınırları korunarak iletilmesi.** İki ayrı OCPP mesajı tek frame'de birleştirilmemeli, tek mesaj birden çok frame'e bölünmemeli. | **Önerilen** | OCPP-J'de her WebSocket mesajı tam bir JSON dizisidir. Mesajların birleşmesi/bölünmesi karşı tarafta JSON parse hatasına yol açar. |
| R6 | **Çok kiracılılık (multi-tenant) için orijinal client host bilgisinin upstream'e iletilmesi** (`X-Forwarded-Host` başlığı ile; varsa proxy zinciri de dikkate alınarak). | **Önerilen** | GoOCPP tenant'ı host bilgisinden çözer ve `X-Forwarded-Host` başlığına öncelik verir. Üretimde load balancer + proxy zincirinde doğru tenant çözümü için orijinal client host'unun korunması gerekir. |

## 4. Kabul kriterleri

Aşağıdakiler sağlandığında talep karşılanmış sayılır:

1. Bir OCPP 1.6 şarj cihazı, `ws://<proxy>/ocpp/{chargePointId}` adresine bağlandığında proxy üzerinden CSMS'e ulaşır ve **BootNotification** başarıyla tamamlanır (CP `Accepted` cevabını proxy üzerinden alır).
2. **CP → CSMS** yönündeki mesajlar (StatusNotification, StartTransaction, MeterValues, StopTransaction) ve bunların cevapları proxy üzerinden çift yönlü akar.
3. **CSMS → CP** yönünde CSMS tarafından başlatılan bir komut (ör. RemoteStartTransaction) proxy üzerinden CP'ye ulaşır ve CP'nin cevabı CSMS'e geri döner.
4. Handshake'te subprotocol `ocpp1.6` olarak doğru negotiate edilir ve bağlantı yolu (`/ocpp/{chargePointId}`) CSMS'e doğru iletilir.

## 5. Ek bilgi

Bu gereksinimler, kervan-proxy'nin GoOCPP CSMS + bir OCPP 1.6 şarj cihazı simülatörü ile birlikte test edilmesi sırasında tespit edilmiştir. R1–R3'ün bağlantının kurulması için zorunlu olduğu; özellikle R3'ün karşılanmaması durumunda CSMS'in bağlantıyı doğrudan **"unsupported protocol"** diyerek reddettiği gözlemlenmiştir. R4 (text frame) test ettiğimiz iki uç için teknik olarak zorunlu olmasa da, spec-uyumu ve katı gerçek cihazlarla uyumluluk için önerilmektedir. Şarj cihazının doğrudan (proxy'siz) bağlanması durumunda tüm akışların hâlihazırda sorunsuz çalıştığı da doğrulanmıştır — yani şeffaflık kısıtı (bkz. Bölüm 1) karşılanabilir bir hedeftir.
