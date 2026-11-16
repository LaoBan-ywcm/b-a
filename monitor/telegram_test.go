package monitor_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/LaoBan-ywcm/b-a/monitor"
)

func TestCheckCombinesRankedAlertsWithoutActiveBatchWait(t *testing.T) {
	f := newFixture(t)
	f.add("BETAUSDT", "150", "1", "2.5", "10", "12")
	f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(context.Context, time.Duration) error { t.Fatal("本轮合并不应主动等待"); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err != nil || len(result.Sent) != 2 || len(f.messages) != 1 {
		t.Fatalf("同轮应合并为一条：sent=%d messages=%d err=%v", len(result.Sent), len(f.messages), err)
	}
	message := f.messages[0]
	if strings.Index(message, "ALPHAUSDT") > strings.Index(message, "BETAUSDT") || strings.Count(message, "检查时间：") != 2 {
		t.Fatalf("消息必须按涨幅排序且保留完整合约结果：%s", message)
	}
}

func TestCheckSplitsCompleteUnicodeBlocksAndPacesMessages(t *testing.T) {
	f := newFixture(t)
	alpha := "ALPHA" + strings.Repeat("🚀", 1000)
	beta := "BETA" + strings.Repeat("甲", 2000)
	f.add(beta, "150", "1", "2.5", "10", "12")
	f.add(alpha, "201", "1", "3.01", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	var waits []time.Duration
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(_ context.Context, d time.Duration) error { waits = append(waits, d); now = now.Add(d); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err != nil || len(result.Sent) != 2 || len(f.messages) != 2 {
		t.Fatalf("需按完整合约拆成两条，sent=%d messages=%d err=%v", len(result.Sent), len(f.messages), err)
	}
	for i, message := range f.messages {
		if len(utf16.Encode([]rune(message))) > 4096 || strings.Count(message, "检查时间：") != 1 {
			t.Fatalf("第 %d 条长度超限或拆散合约", i)
		}
	}
	if !strings.Contains(f.messages[0], alpha) || !strings.Contains(f.messages[1], beta) {
		t.Fatal("拆分后合约顺序或内容丢失")
	}
	if len(waits) != 1 || waits[0] < time.Second {
		t.Fatalf("同一私聊需每秒最多一条，waits=%v", waits)
	}
}

func TestCheckRetriesTemporaryTelegramFailuresWithBoundedWaits(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		response     string
		recover      bool
		wantAttempts int
		wantWait     time.Duration
	}{
		{"429_http", 429, `{"ok":false,"error_code":429,"parameters":{"retry_after":5}}`, true, 2, 5 * time.Second},
		{"429_ok_false", 200, `{"ok":false,"error_code":429,"parameters":{"retry_after":5}}`, true, 2, 5 * time.Second},
		{"temporary_http", 503, `TEST_SECRET`, true, 2, time.Second},
		{"temporary_ok_false", 200, `{"ok":false,"error_code":500}`, true, 2, time.Second},
		{"permanent_http", 401, `TEST_SECRET`, false, 1, 0},
		{"permanent_ok_false", 200, `{"ok":false,"error_code":403,"description":"TEST_SECRET"}`, false, 1, 0},
		{"exhausted", 503, `TEST_SECRET`, false, 3, 3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
			now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
			attempts := 0
			var waited time.Duration
			tg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts++
				if tc.recover && attempts > 1 {
					io.WriteString(w, `{"ok":true}`)
					return
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.response)
			}))
			defer tg.Close()
			cfg := f.config()
			cfg.TelegramBaseURL = tg.URL
			var logs strings.Builder
			app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(&logs, "", 0), Wait: func(_ context.Context, d time.Duration) error { waited += d; now = now.Add(d); return nil }})
			if err != nil {
				t.Fatal(err)
			}
			result, err := app.Check(context.Background())
			if attempts != tc.wantAttempts || waited != tc.wantWait {
				t.Fatalf("attempts=%d wait=%v，期望 %d / %v", attempts, waited, tc.wantAttempts, tc.wantWait)
			}
			if tc.recover {
				if err != nil || len(result.Sent) != 1 {
					t.Fatalf("重试成功未计入：%+v %v", result, err)
				}
			} else {
				if err == nil || len(result.Sent) != 0 {
					t.Fatalf("失败不得冷却：%+v %v", result, err)
				}
			}
			if strings.Contains(fmt.Sprint(err)+logs.String(), "TEST_SECRET") {
				t.Fatal("错误泄露 token")
			}
		})
	}
}

func TestCheckPreservesLongTelegramLimitAcrossRoundsAndRechecksMarket(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	start := now
	attempts := 0
	tg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 2 {
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), "Telegram 通知恢复") || strings.Contains(string(body), "ALPHAUSDT") {
				t.Errorf("只能汇报链路恢复，不得重放旧上涨提醒：%s", body)
			}
		}
		if attempts == 1 {
			w.WriteHeader(429)
			io.WriteString(w, `{"ok":false,"parameters":{"retry_after":120}}`)
			return
		}
		if now.Before(start.Add(120 * time.Second)) {
			t.Error("提前越过跨轮限流")
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	defer tg.Close()
	cfg := f.config()
	cfg.TelegramBaseURL = tg.URL
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(context.Context, time.Duration) error {
		t.Fatal("长于有效期的等待必须直接结束本轮")
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, elapsed := range []time.Duration{0, 30 * time.Second, time.Minute} {
		now = start.Add(elapsed)
		result, err := app.Check(context.Background())
		if err == nil || len(result.Sent) != 0 || attempts != 1 {
			t.Fatalf("限流后旧信号不得提前发送：attempts=%d sent=%d err=%v", attempts, len(result.Sent), err)
		}
	}
	now = start.Add(2 * time.Minute)
	f.tickers[0]["priceChangePercent"] = "90"
	result, err := app.Check(context.Background())
	if err != nil || len(result.Sent) != 0 || attempts != 2 {
		t.Fatalf("恢复后必须重新筛选，无效旧信号不得补发：%+v %v", result, err)
	}
	// 第一次恢复请求只汇报通知链路恢复；下一次上涨仍须遵守共享的一秒速率。
	now = now.Add(time.Second)
	f.tickers[0]["priceChangePercent"] = "201"
	result, err = app.Check(context.Background())
	if err != nil || len(result.Sent) != 1 || attempts != 3 {
		t.Fatalf("新行情符合后应发送：%+v %v", result, err)
	}
}

func TestCheckExpiresSignalsAtEarlierOfCheckIntervalAndOneMinute(t *testing.T) {
	for _, interval := range []time.Duration{10 * time.Second, 2 * time.Minute} {
		t.Run(interval.String(), func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
			now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
			f.onExchange = func() { now = now.Add(min(interval, time.Minute)) }
			cfg := f.config()
			cfg.CheckInterval = interval
			app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)})
			if err != nil {
				t.Fatal(err)
			}
			result, err := app.Check(context.Background())
			if err == nil || len(result.Sent) != 0 || len(f.messages) != 0 {
				t.Fatalf("行情采集已耗尽有效期，不得发送：%+v %v", result, err)
			}
		})
	}
}

func TestCheckCancelDuringSplitWaitPreservesAlreadyDeliveredCooldown(t *testing.T) {
	f := newFixture(t)
	alpha := "ALPHA" + strings.Repeat("甲", 1900)
	beta := "BETA" + strings.Repeat("乙", 1900)
	f.add(alpha, "201", "1", "3.01", "10", "12")
	f.add(beta, "150", "1", "2.5", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := f.config()
	opts := monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(context.Context, time.Duration) error { cancel(); return ctx.Err() }}
	app, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(ctx)
	if !errors.Is(err, context.Canceled) || len(result.Sent) != 1 || result.Sent[0].Symbol != alpha || len(f.messages) != 1 {
		t.Fatalf("等待取消应仅保留成功批次：sent=%d messages=%d err=%v", len(result.Sent), len(f.messages), err)
	}
	restarted, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 1 || result.Sent[0].Symbol != beta {
		t.Fatalf("取消后重启只应发送未确认批次：sent=%d err=%v", len(result.Sent), err)
	}
}

func TestCheckRetainsLastRetryAfterEvenWhenAttemptsExhausted(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	attempts := 0
	tg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(429)
		if attempts < 3 {
			io.WriteString(w, `{"ok":false,"parameters":{"retry_after":1}}`)
		} else {
			io.WriteString(w, `{"ok":false,"parameters":{"retry_after":120}}`)
		}
	}))
	defer tg.Close()
	cfg := f.config()
	cfg.TelegramBaseURL = tg.URL
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := app.Check(context.Background()); err == nil || len(result.Sent) != 0 || attempts != 3 {
		t.Fatalf("应耗尽三次尝试：attempts=%d sent=%d err=%v", attempts, len(result.Sent), err)
	}
	now = now.Add(30 * time.Second)
	if result, err := app.Check(context.Background()); err == nil || len(result.Sent) != 0 || attempts != 3 {
		t.Fatalf("最后一次响应的 retry_after 仍须跨轮保留：attempts=%d sent=%d err=%v", attempts, len(result.Sent), err)
	}
}

func TestCheckUnknownTelegramResultDoesNotPersistCooldown(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	attempts := 0
	recovering := false
	tg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if recovering {
			io.WriteString(w, `{"ok":true}`)
		} else {
			io.WriteString(w, `{"ok":true,"description":"TEST_SECRET"`)
		}
	}))
	defer tg.Close()
	cfg := f.config()
	cfg.TelegramBaseURL = tg.URL
	opts := monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }}
	app, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err == nil || attempts != 3 || len(result.Sent) != 0 || strings.Contains(err.Error(), "TEST_SECRET") {
		t.Fatalf("结果不明需有界重试且不冷却：attempts=%d sent=%d err=%v", attempts, len(result.Sent), err)
	}
	recovering = true
	restarted, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 1 || attempts != 4 {
		t.Fatalf("结果不明不得持久化成功：attempts=%d sent=%d err=%v", attempts, len(result.Sent), err)
	}
}

func TestCheckRejectsAnOversizedContractWithoutTruncatingItsEvidence(t *testing.T) {
	f := newFixture(t)
	f.add(strings.Repeat("🚀", 2100), "201", "1", "3.01", "10", "12")
	result, err := f.check(t, f.config())
	if err == nil || len(result.Sent) != 0 || len(f.messages) != 0 {
		t.Fatalf("单合约超限不得截断证据或发送非法消息：sent=%d messages=%d err=%v", len(result.Sent), len(f.messages), err)
	}
}
