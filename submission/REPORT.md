# K4-Track02-Day17 — Report cá nhân

**Họ tên:** Hoàng Phong

**MSSV:** 2A202602943

**Repo:** https://github.com/HoangPhong20/K4-Track02-Day17-HoangPhong-2A202602943-Data-Pipeline-Engineering

**Commit mã nguồn dùng để kiểm tra:** `ca4acb270571715f08e14710cd58a438a96820d0`

**Ngày kiểm tra:** 2026-10-05

**AI và phạm vi hỗ trợ:** Codex hỗ trợ đọc code/tài liệu, sửa MERGE, lookback và CDC delete, chạy kiểm tra, ghi output và soạn REPORT. Học viên chịu trách nhiệm review và giải thích thay đổi.

**Nguồn tham khảo:** README.md, docs/CHECKPOINTS.md, docs/RULES.md và docs/SUBMISSION.md của repo; không dùng nguồn ngoài.

## 1. Ba lỗi

**Silver:** Ticket có nhiều hàng, trạng thái T-91 không bảo đảm mới nhất. Nguyên nhân: INSERT từng batch; dedup nội bộ không ngăn trùng giữa batch. Sửa `pipeline/silver.py`: MERGE theo ticket_id, chỉ UPDATE khi source._lsn > target._lsn; giữ dedup theo LSN. Khái niệm: khoá thực thể, upsert idempotent, thứ tự CDC.

**Late data:** Gold lệch full recompute; u05 ngày 08-12 có (2 events, 1 click, 0 feedback down), cần (5, 3, 1). Nguyên nhân: lookback=0 bỏ event đến ngày 08-15. Sửa `pipeline/config.py`: LOOKBACK_DAYS=3; logic sẵn có tính lại [day−3, day]. Khái niệm: event time, ingest time, recompute partition.

**CDC delete:** T-97 còn ở Silver, snapshot mới nhất và RAG. Nguyên nhân: lấy khoá từ after=null làm mất delete. Sửa `pipeline/staging.py`: coalesce khoá từ key/after/before; giữ op và LSN, trường cá nhân đọc từ after nên null. Logic sẵn có truyền xoá xuống Gold. Khái niệm: CDC delete khác Kafka tombstone; xoá phải lan.

## 2. Các con số

43 bản ghi Bronze: P50=0, P95=2,90, **P99=3**, max=3 ngày; chọn lookback=ceil(P99)=3, bao phủ seed nhưng cần đo lại khi dữ liệu thay đổi. Verify **18/18**, pytest **34 passed**. Rerun **PASS**, C0=C1=C2=C3=`39e115c510ecdf526800eac227158a4f` (Gold). dbt **PASS=19**, parity **PARITY**; checksum prefix Silver `3c15dfd43701`, feature `8630e04a61d1`. Parity chỉ so hai bảng chung.

## 3. Lựa chọn kỹ thuật

MERGE theo khoá/LSN giữ trạng thái mới nhất khi replay; overwrite partition tính lại feature từ Silver nên không cộng trùng. Tombstone giữ ticket_id và LSN chống hồi sinh, bỏ dữ liệu cá nhân. Snapshot versioned dựng từ Bronze as-of ngày đó giữ point-in-time và khả năng tái lập; dữ liệu muộn tạo phiên bản mới. DuckDB phù hợp seed nhỏ, chạy local; dbt quản lý dependency SQL, contract kiểu dữ liệu của silver_tickets và tests; microbatch theo ngày dùng lookback=3.

## 4. Hai câu hỏi suy ngẫm

**Snapshot và xoá:** Lab giữ snapshot cũ, chỉ loại T-97 khỏi snapshot mới nhất/RAG. Với hệ thống thực, tôi coi xoá là ngoại lệ có kiểm soát: chặn truy cập phiên bản chứa PII, tạo bản đã loại dữ liệu và thu hồi bản cũ; lan xoá tới Bronze, transcript, cache, export và backup theo chính sách. Giữ audit không chứa PII, áp dụng danh sách xoá khi replay/restore; đánh giá model đã học dữ liệu đó để retrain khi cần.

**Chốt PII:** Đặt regex kết hợp NER nhận diện tên tiếng Việt ở Bronze→Silver, bao phủ subject/body/transcript; bản nghi ngờ vào quarantine, kiểm tra lại trước Gold/embedding. Bronze hạn chế quyền truy cập. Đo precision/recall theo loại PII trên tập gán nhãn, ưu tiên recall; theo dõi tỷ lệ rò rỉ, che nhầm và kiểm thử hồi quy với tên có dấu, không dấu, ngữ cảnh khó. Đây là đề xuất bổ sung, chưa triển khai trong lab.

## 5. Output thực tế

Các output dưới đây sinh từ mã nguồn của commit nêu trên. dbt build chạy ở CP5 trong cùng phiên làm việc trước khi tạo commit; các file mã nguồn và model dbt không đổi. Đã bỏ mã màu ANSI và khoảng trắng cuối dòng của terminal. Bằng chứng rerun cũng được lưu trong `submission/checksums.txt`.

```text
$ .\.venv\Scripts\python.exe -X utf8 -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

```text
$ .\.venv\Scripts\python.exe -X utf8 -m pytest
..................................                                       [100%]
34 passed in 3.55s
```

```text
$ .\.venv\Scripts\python.exe -X utf8 -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

```text
$ .\.venv\Scripts\python.exe -X utf8 main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

Dependencies đã có sẵn: dbt-core 1.12.5, dbt-duckdb 1.11.0; lệnh cài đặt `.\.venv\Scripts\python.exe -m pip install -r requirements-dbt.txt` thành công.

```text
$ .\.venv\Scripts\python.exe -X utf8 main.py --land-only
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)
```

```text
$ $env:DO_NOT_TRACK = '1'
$ $env:PYTHONUTF8 = '1'
$ Push-Location dbt_project
$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
03:03:54  Running with dbt=1.12.5
03:03:55  Registered adapter: duckdb=1.11.0
03:03:56  Unable to do partial parsing because saved manifest not found. Starting full parse.
03:04:02  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:04:02

03:04:02  Concurrency: 1 threads (target='dev')
03:04:02

03:04:06  1 of 19 START sql view model main.stg_events ................................... [RUN]
03:04:07  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.15s]
03:04:07  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
03:04:07  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.07s]
03:04:07  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
03:04:07  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.17s]
03:04:07  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
03:04:07  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.37s]
03:04:07  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
03:04:07  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.19s]
03:04:07  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
03:04:08  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.10s]
03:04:08  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
03:04:08  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
03:04:08  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
03:04:08  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.04s]
03:04:08  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
03:04:08  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
03:04:08  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
03:04:08  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
03:04:08  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
03:04:08  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.05s]
03:04:08  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
03:04:08  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.05s]
03:04:08  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
03:04:08  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.06s]
03:04:08  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
03:04:08  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.05s]
03:04:08  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
03:04:08  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.05s]
03:04:08  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
03:04:08  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
03:04:08  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.07s]
03:04:08  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
03:04:08  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.11s]
03:04:08  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
03:04:08  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.07s]
03:04:08  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
03:04:08  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.05s]
03:04:08  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
03:04:08  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.07s]
03:04:08  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
03:04:09  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
03:04:09  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
03:04:09  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.07s]
03:04:09  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.58s]
03:04:09  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
03:04:09  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
03:04:09  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
03:04:09  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.05s]
03:04:09  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
03:04:09  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.04s]
03:04:09

03:04:09  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 7.15 seconds (7.15s).
03:04:09

03:04:09  Completed successfully
03:04:09

03:04:09  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
$ Pop-Location
```

```text
$ .\.venv\Scripts\python.exe -X utf8 -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
