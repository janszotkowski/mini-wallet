# mini-wallet

Malá služba v Go, která převádí peníze mezi účty. Nejdůležitější na ní není počet funkcí, ale správnost.

Služba zaručuje, že:

- peníze nikdy nevzniknou ani nezmizí, ani když běží mnoho převodů současně,
- žádný účet se nedostane do mínusu,
- opakovaný požadavek se stejným `Idempotency-Key` nikdy nepřevede peníze podruhé,
- každý dokončený převod se oznámí jako event `transfer.completed` v Kafce a event se neztratí ani při pádu procesu.

## Jak to funguje

```mermaid
flowchart LR
    client([Klient])

    subgraph app [mini-wallet]
        api["REST API<br/>cmd/api<br/>:8080"]
        grpc["gRPC server<br/>cmd/grpc<br/>:9090"]
        ledger["internal/ledger<br/>Transfer()"]
        publisher["Outbox publisher<br/>cmd/publisher"]
    end

    subgraph pg [PostgreSQL 18]
        accounts[(accounts)]
        transfers[(transfers)]
        entries[(ledger_entries)]
        outbox[(outbox_events)]
    end

    kafka[["Kafka<br/>topic transfer.completed"]]
    consumers([Další služby])

    client -- "POST /transfers" --> api
    client -- "CreateTransfer" --> grpc
    api --> ledger
    grpc --> ledger
    ledger -- "jedna transakce" --> accounts
    ledger --> transfers
    ledger --> entries
    ledger --> outbox
    publisher -- "každou sekundu:<br/>čte neodeslané" --> outbox
    publisher -- "odešle" --> kafka
    kafka --> consumers
```

- **REST a gRPC** jsou dva vstupy do stejné logiky. Oba zkontrolují požadavek a zavolají `ledger.Transfer`. Logika převodu peněz je jen na jednom místě.
- **`ledger.Transfer`** zapíše všechno v **jedné databázové transakci**: nové zůstatky, dva záznamy v ledgeru, záznam o převodu a event do outboxu. Uloží se buď všechno, nebo nic.
- **Publisher** je samostatný proces. Každou sekundu načte eventy, které ještě nebyly odeslané, pošle je do Kafky a označí je jako odeslané.

## Jeden převod krok po kroku

```mermaid
sequenceDiagram
    autonumber
    participant C as Klient
    participant A as REST / gRPC
    participant L as ledger.Transfer
    participant DB as PostgreSQL

    C->>A: převeď 300 z A na B<br/>Idempotency-Key: k1
    A->>A: kontrola klíče, UUID, JSON
    A->>L: Transfer(k1, A, B, 300)
    L->>DB: BEGIN
    L->>DB: SELECT ... WHERE id IN (A, B)<br/>ORDER BY id FOR UPDATE
    Note over DB: oba účty zamčené,<br/>vždy ve stejném pořadí
    L->>DB: načtení zůstatků A a B
    L->>DB: INSERT INTO transfers ...<br/>ON CONFLICT (idempotency_key) DO NOTHING
    alt klíč už byl použitý
        DB-->>L: žádný nový řádek
        L->>DB: načtení existujícího převodu
        L-->>A: stejné transferId, replayed = true
        A-->>C: 200 OK
    else nový klíč
        L->>L: má A aspoň 300?
        L->>DB: UPDATE zůstatků A a B
        L->>DB: INSERT 2 záznamů do ledgeru (-300, +300)
        L->>DB: INSERT eventu transfer.completed do outboxu
        L->>DB: COMMIT
        L-->>A: nové transferId
        A-->>C: 201 Created
    end
```

Když A nemá dost peněz, transakce se zruší (rollback) a neuloží se nic, ani záznam v `transfers`.

## Proč je to správně

### Souběžné převody: zámky řádků ve stálém pořadí

Bez zámků si dva převody můžou přečíst stejný zůstatek (třeba 1000), oba odečtou a jedna změna se ztratí. Přesně to ukázal race test: součet všech zůstatků byl 5050 místo 5000.

`SELECT ... FOR UPDATE` zamkne oba účty až do konce transakce, takže druhý převod na stejném účtu počká.

Zamykání **v pořadí podle ID účtů** brání deadlockům.

Bez stálého pořadí na sebe dva převody v opačném směru čekají navždy:

```mermaid
flowchart LR
    t1["Převod A → B<br/>zamkne A, čeká na B"]
    t2["Převod B → A<br/>zamkne B, čeká na A"]
    t1 -. čeká .-> t2
    t2 -. čeká .-> t1
```

S `ORDER BY id` oba začnou stejným účtem, takže jeden prostě počká na druhého:

```mermaid
flowchart LR
    t3["Převod A → B<br/>zamkne A, pak B"]
    t4["Převod B → A<br/>také nejdřív zamyká A, čeká"]
    t3 -- "commit, A odemčeno" --> t4
```

Omezení `CHECK (balance >= 0)` je poslední pojistka: ani chyba v kódu neuloží záporný zůstatek.

### Podvojné účetnictví (double-entry ledger)

Každý převod zapíše dva záznamy do `ledger_entries`: `-částka` u odesílatele a `+částka` u příjemce. Jejich součet je vždy 0 a zůstatek každého účtu se rovná počátečnímu zůstatku plus součtu jeho záznamů. Race test obě pravidla ověřuje po 100 souběžných převodech.

### Idempotence

`Idempotency-Key` se ukládá do tabulky `transfers` s omezením `UNIQUE`.

| Požadavek | Výsledek |
|---|---|
| Nový klíč | Převod proběhne, `201 Created` |
| Stejný klíč, stejné parametry | Nic se nepřevede, vrátí se původní `transferId`, `200 OK` |
| Stejný klíč, jiné parametry | `422 Unprocessable Entity` |
| Neúspěšný pokus (třeba málo peněz) | Nic se neuloží, stejný klíč jde použít znovu |

Test posílá 20 požadavků se stejným klíčem současně. Vznikne přesně jeden převod a zbylých 19 vrátí původní `transferId`.

### Transakční outbox

Zápis do databáze a do Kafky nejde provést jako jednu nedělitelnou operaci. Kdyby služba posílala event rovnou:

- poslání **po** `COMMIT` event ztratí, když proces mezitím spadne,
- poslání **před** `COMMIT` oznámí převod, který možná vůbec neproběhne.

Event se proto uloží do `outbox_events` **ve stejné transakci** jako převod a publisher ho doručí později:

```mermaid
flowchart LR
    subgraph tx [Jedna databázová transakce]
        balances[Úprava zůstatků] --> ledger[Záznamy v ledgeru] --> event["Event v outboxu<br/>published_at = NULL"]
    end
    event --> pub["Publisher<br/>SELECT ... FOR UPDATE SKIP LOCKED"]
    pub -- "1. odeslání" --> kafka[[Kafka]]
    pub -- "2. published_at = now()" --> done[Event vyřízen]
```

Doručení je **at-least-once** (alespoň jednou): když publisher spadne poté, co Kafka zprávu přijala, ale dřív, než event označil jako odeslaný, pošle se event znovu. Každá zpráva má jako klíč ID eventu, takže příjemci můžou duplicity přeskočit. Díky `SKIP LOCKED` může běžet víc publisherů najednou a žádný event se nepošle dvakrát současně.

## Datový model

```mermaid
erDiagram
    accounts ||--o{ ledger_entries : "má"
    accounts ||--o{ transfers : "odesílá"
    accounts ||--o{ transfers : "přijímá"
    transfers ||--|{ ledger_entries : "vytvoří 2"

    accounts {
        uuid id PK
        text name
        bigint balance "CHECK >= 0"
        timestamptz created_at
    }
    transfers {
        uuid id PK
        text idempotency_key "UNIQUE"
        uuid from_account_id FK
        uuid to_account_id FK
        bigint amount "CHECK > 0"
        timestamptz created_at
    }
    ledger_entries {
        uuid id PK
        uuid transfer_id
        uuid account_id FK
        bigint amount "CHECK <> 0"
        timestamptz created_at
    }
    outbox_events {
        uuid id PK
        text event_type
        jsonb payload
        timestamptz created_at
        timestamptz published_at "NULL = ještě neodesláno"
    }
```

- Všechna ID jsou **UUIDv7** generovaná PostgreSQL 18 (`uuidv7()`), takže se dají řadit podle času.
- Částky jsou `BIGINT` v nejmenších jednotkách měny (haléřích), nikdy desetinná čísla.

## Technologie

| Oblast | Použito |
|---|---|
| Jazyk | Go 1.26, router ze standardní knihovny `net/http` |
| Databáze | PostgreSQL 18, `pgx` v5, migrace přes `goose` |
| Eventy | Kafka 4 (KRaft, bez ZooKeeperu), klient `franz-go` |
| RPC | gRPC a Protocol Buffers, kód generovaný přes `buf` |
| Běh | Docker multi-stage build (image distroless), Docker Compose |

## Spuštění

Spustí všechno: PostgreSQL, Kafku, migrace, REST API, gRPC server a publisher.

```bash
docker compose up -d --build
```

Endpoint pro založení účtu neexistuje, takže dva účty vlož přes SQL:

```bash
docker compose exec postgres psql -U wallet -d wallet -c "INSERT INTO accounts (id, name, balance) VALUES ('11111111-1111-7111-8111-111111111111', 'alice', 1000), ('22222222-2222-7222-8222-222222222222', 'bob', 0)"
```

### REST

```bash
curl -i -X POST localhost:8080/transfers \
  -H 'Idempotency-Key: example-1' \
  -d '{"fromAccountId":"11111111-1111-7111-8111-111111111111","toAccountId":"22222222-2222-7222-8222-222222222222","amount":300}'
```

```bash
curl -i localhost:8080/health
```

### gRPC

```bash
go run github.com/fullstorydev/grpcurl/cmd/grpcurl@latest -plaintext \
  -d '{"idempotency_key":"example-2","from_account_id":"11111111-1111-7111-8111-111111111111","to_account_id":"22222222-2222-7222-8222-222222222222","amount":100}' \
  localhost:9090 wallet.v1.WalletService/CreateTransfer
```

### Eventy v Kafce

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transfer.completed --from-beginning
```

### Chyby

| Situace | REST | gRPC |
|---|---|---|
| Nový převod | `201 Created` | OK, `replayed` chybí |
| Opakování se stejným klíčem | `200 OK` | OK, `replayed: true` |
| Chybějící nebo neplatný klíč, neplatný JSON nebo UUID, částka <= 0, stejný účet | `400 Bad Request` | `InvalidArgument` |
| Účet neexistuje | `404 Not Found` | `NotFound` |
| Málo peněz, klíč použitý s jinými parametry | `422 Unprocessable Entity` | `FailedPrecondition` |
| Cokoli jiného | `500 Internal Server Error` | `Internal` |

## Testy

Testy běží proti skutečnému PostgreSQL. Se spuštěným `docker compose up`:

```bash
DATABASE_URL="postgres://wallet:wallet@localhost:5433/wallet?sslmode=disable" go test -race ./...
```

| Test | Co dokazuje |
|---|---|
| `TestTransfer` | zůstatky i dva záznamy v ledgeru jsou správně |
| `TestTransferInsufficientFunds` | účet nejde do mínusu a nic se nezmění |
| `TestTransferConcurrent` | 100 souběžných náhodných převodů nezmění součet a každý zůstatek sedí s ledgerem |
| `TestTransferSameKeyConcurrent` | 20 souběžných požadavků s jedním klíčem vytvoří přesně jeden převod |
| `TestTransferWritesOutboxEvent` | převod zapíše jeden event do outboxu, opakování další nezapíše |

## Struktura projektu

```
cmd/
  api/          REST server: routing, JSON, HTTP status kódy
  grpc/         gRPC server: převod požadavků a chyb na gRPC
  publisher/    každou sekundu posílá eventy z outboxu do Kafky
internal/
  ledger/       logika převodu a její testy
  outbox/       načítá neodeslané eventy a posílá je
proto/          popis gRPC API v Protocol Buffers
gen/            Go kód vygenerovaný z proto/ (neupravovat ručně)
migrations/     SQL migrace pro goose
Dockerfile      sestaví všechny programy do jednoho malého image
compose.yaml    spustí celý systém lokálně
```
