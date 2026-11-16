package monitor_test

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LaoBan-ywcm/b-a/monitor"
)

func TestCheckRecoversTransientMarketFailureWithBoundedBackoff(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
	attempts := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/ticker/24hr" {
			attempts++
			if attempts < 3 {
				w.WriteHeader(503)
				return
			}
		}
		f.server.Config.Handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	cfg := f.config()
	cfg.BinanceBaseURL = proxy.URL
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	var waits []time.Duration
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(ctx context.Context, d time.Duration) error {
		waits = append(waits, d)
		now = now.Add(d)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err != nil || len(result.Sent) != 1 {
		t.Fatalf("短暂失败后应完成当前检查：%+v %v", result, err)
	}
	if len(waits) != 2 || waits[0] != time.Second || waits[1] != 2*time.Second {
		t.Fatalf("有界退避 = %v", waits)
	}
}

func TestMarketRateLimitIsSharedAcrossCandidatesAndRoundsAndRefreshesOnRecovery(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		retryAfter string
		delay      time.Duration
	}{
		{"429_with_header", 429, "120", 2 * time.Minute},
		{"429_without_header", 429, "", time.Minute},
		{"418_without_header", 418, "", 2 * time.Minute},
		{"http_date", 429, "Mon, 12 Jan 2026 01:02:00 GMT", 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.add("GOODUSDT", "400", "1", "5", "10", "12")
			f.add("LIMITEDUSDT", "300", "1", "4", "10", "12")
			f.add("LATERUSDT", "200", "1", "3", "10", "12")
			now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
			limited := true
			requestsDuringBan := 0
			var banUntil time.Time
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !banUntil.IsZero() && now.Before(banUntil) {
					requestsDuringBan++
					w.WriteHeader(418)
					return
				}
				if limited && r.URL.Query().Get("symbol") == "LIMITEDUSDT" {
					limited = false
					banUntil = now.Add(tc.delay)
					w.Header().Set("Retry-After", tc.retryAfter)
					w.WriteHeader(tc.status)
					return
				}
				f.server.Config.Handler.ServeHTTP(w, r)
			}))
			defer proxy.Close()
			cfg := f.config()
			cfg.BinanceBaseURL = proxy.URL
			app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(_ context.Context, delay time.Duration) error {
				// 健康通知可等待共享 Telegram 速率；Binance 不得等待整段封禁后沿用旧快照。
				if delay > time.Second {
					t.Fatalf("行情限流不得在本轮等待：%v", delay)
				}
				now = now.Add(delay)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := app.Check(context.Background())
			if err == nil || len(result.Sent) != 1 || result.Sent[0].Symbol != "GOODUSDT" {
				t.Fatalf("限流前完整候选可通知；其余候选未知：%+v %v", result, err)
			}
			if result.MarketStatus != monitor.MarketPartial || len(result.MarketFailures) != 2 || !result.MarketFailures[0].Global || !result.MarketFailures[1].Global {
				t.Fatalf("限流影响全局，但应保留两个未完成候选：%+v", result)
			}
			now = now.Add(tc.delay - time.Nanosecond)
			result, err = app.Check(context.Background())
			if err == nil || len(result.Sent) != 0 || requestsDuringBan != 0 {
				t.Fatalf("共享限流闸门失效：%+v %v 违规请求=%d", result, err, requestsDuringBan)
			}
			now = now.Add(time.Nanosecond)
			for _, tick := range f.tickers {
				tick["priceChangePercent"] = "99"
			}
			result, err = app.Check(context.Background())
			if err != nil || len(result.Sent) != 0 || len(result.Alerts) != 0 {
				t.Fatalf("恢复必须重查当前条件，不补发旧行情：%+v %v", result, err)
			}
		})
	}
}

func TestCheckDistinguishesGlobalPartialAndCompleteMarketResults(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*fixture)
		status   monitor.MarketStatus
		failures int
		wantSent int
	}{
		{"global_http", func(f *fixture) { f.failures["/fapi/v1/time"] = 401 }, monitor.MarketFailed, 1, 0},
		{"missing_ticker", func(f *fixture) { f.tickers = f.tickers[1:] }, monitor.MarketFailed, 1, 0},
		{"invalid_ranking", func(f *fixture) { f.tickers[0]["priceChangePercent"] = "NaN" }, monitor.MarketFailed, 1, 0},
		{"candidate_http", func(f *fixture) { f.failures["/fapi/v1/klines/ALPHAUSDT"] = 401 }, monitor.MarketPartial, 1, 1},
		{"candidate_price", func(f *fixture) { f.tickers[0]["lastPrice"] = "0" }, monitor.MarketPartial, 1, 1},
		{"invalid_price_below_threshold", func(f *fixture) { f.tickers[0]["priceChangePercent"] = "50"; f.tickers[0]["lastPrice"] = "0" }, monitor.MarketPartial, 1, 1},
		{"duplicate_ticker", func(f *fixture) { f.tickers = append(f.tickers, f.tickers[0]) }, monitor.MarketFailed, 1, 0},
		{"duplicate_metadata", func(f *fixture) { f.metadata = append(f.metadata, f.metadata[0]) }, monitor.MarketFailed, 1, 0},
		{"missing_metadata_field", func(f *fixture) { f.metadata[0]["status"] = "" }, monitor.MarketFailed, 1, 0},
		{"candidate_week", func(f *fixture) { f.weeks["ALPHAUSDT"][1][4] = "NaN" }, monitor.MarketPartial, 1, 1},
		{"no_closed_week", func(f *fixture) { f.weeks["ALPHAUSDT"] = f.weeks["ALPHAUSDT"][2:] }, monitor.MarketComplete, 0, 1},
		{"no_signal", func(f *fixture) {
			for _, tick := range f.tickers {
				tick["priceChangePercent"] = "50"
			}
		}, monitor.MarketComplete, 0, 0},
		{"notification_failure", func(f *fixture) { f.telegramResponse = `{"ok":false}` }, monitor.MarketComplete, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", "300", "1", "4", "10", "12")
			f.add("BETAUSDT", "200", "1", "3", "10", "12")
			tc.change(f)
			result, err := f.check(t, f.config())
			if result.MarketStatus != tc.status || len(result.MarketFailures) != tc.failures || len(result.Sent) != tc.wantSent {
				t.Fatalf("检查结果分类不正确：%+v %v", result, err)
			}
			if tc.failures > 0 && err == nil {
				t.Fatal("失败不能当作普通无信号结果")
			}
			if tc.status == monitor.MarketPartial && (result.MarketFailures[0].Symbol != "ALPHAUSDT" || result.MarketFailures[0].Global) {
				t.Fatalf("应保留失败合约：%+v", result.MarketFailures)
			}
		})
	}
}

func TestCheckBoundsMarketRetriesAndPreservesCancellation(t *testing.T) {
	for _, name := range []string{"server_error", "timeout", "permanent", "cancel_wait"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
			// 超时返回时服务端处理器可能仍在退出，计数跨 HTTP goroutine 读取，必须同步。
			var attempts atomic.Int64
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				if name == "timeout" {
					<-r.Context().Done()
					return
				}
				if name == "permanent" {
					w.WriteHeader(400)
					return
				}
				w.WriteHeader(503)
			}))
			defer proxy.Close()
			cfg := f.config()
			cfg.BinanceBaseURL = proxy.URL
			cfg.HTTPTimeout = 50 * time.Millisecond
			now := time.Now()
			waits := 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(ctx context.Context, d time.Duration) error {
				waits++
				if name == "cancel_wait" {
					cancel()
					return ctx.Err()
				}
				now = now.Add(d)
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := app.Check(ctx)
			if err == nil || result.MarketStatus != monitor.MarketFailed || len(result.Sent) != 0 {
				t.Fatalf("请求失败必须可观察：%+v %v", result, err)
			}
			wantAttempts, wantWaits := 3, 2
			if name == "permanent" {
				wantAttempts, wantWaits = 1, 0
			}
			if name == "cancel_wait" {
				wantAttempts, wantWaits = 1, 1
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("取消原因丢失：%v", err)
				}
			}
			if attempts.Load() != int64(wantAttempts) || waits != wantWaits {
				t.Fatalf("重试未受控：请求=%d 等待=%d", attempts.Load(), waits)
			}
		})
	}
}

func TestCheckRejectsInvalidGlobalAndWeeklySnapshots(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		want             monitor.MarketStatus
	}{
		{"missing_metadata", "/fapi/v1/exchangeInfo", `{}`, monitor.MarketFailed},
		{"null_metadata", "/fapi/v1/exchangeInfo", `{"symbols":null}`, monitor.MarketFailed},
		{"null_tickers", "/fapi/v1/ticker/24hr", `null`, monitor.MarketFailed},
		{"missing_time", "/fapi/v1/time", `{}`, monitor.MarketFailed},
		{"invalid_json", "/fapi/v1/time", `TEST_SECRET`, monitor.MarketFailed},
		{"trailing_json", "/fapi/v1/time", `{"serverTime":1768176000000} {}`, monitor.MarketFailed},
		{"null_weeks", "/fapi/v1/klines", `null`, monitor.MarketPartial},
		{"error_object", "/fapi/v1/klines", `{"code":-1,"msg":"TEST_SECRET"}`, monitor.MarketPartial},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == tc.path {
					io.WriteString(w, tc.body)
					return
				}
				f.server.Config.Handler.ServeHTTP(w, r)
			}))
			defer proxy.Close()
			cfg := f.config()
			cfg.BinanceBaseURL = proxy.URL
			result, err := f.check(t, cfg)
			if err == nil || result.MarketStatus != tc.want || len(result.Sent) != 0 || len(result.MarketFailures) != 1 {
				t.Fatalf("异常响应不可当作完整检查：%+v %v", result, err)
			}
			if strings.Contains(err.Error(), "TEST_SECRET") {
				t.Fatalf("原始响应泄露：%v", err)
			}
		})
	}
}

func TestMarketRetryAfterOnTemporaryFailureAlsoDefersFreshRound(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	requests := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Retry-After", "90")
		w.WriteHeader(503)
	}))
	defer proxy.Close()
	cfg := f.config()
	cfg.BinanceBaseURL = proxy.URL
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(context.Context, time.Duration) error {
		t.Fatal("不得缩短服务端等待时间重试")
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err == nil || result.MarketStatus != monitor.MarketFailed {
		t.Fatalf("应记录暂不可用：%+v %v", result, err)
	}
	now = now.Add(time.Minute)
	if _, err = app.Check(context.Background()); err == nil || requests != 1 {
		t.Fatalf("503 Retry-After 必须跨轮次生效，请求=%d err=%v", requests, err)
	}
}

func TestCheckRetriesTimeoutWhileReadingMarketBody(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
	attempts := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fapi/v1/ticker/24hr" {
			attempts++
			if attempts == 1 {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				return
			}
		}
		f.server.Config.Handler.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	cfg := f.config()
	cfg.BinanceBaseURL = proxy.URL
	cfg.HTTPTimeout = 50 * time.Millisecond
	now := time.Now()
	waits := 0
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(ctx context.Context, d time.Duration) error { waits++; now = now.Add(d); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err != nil || result.MarketStatus != monitor.MarketComplete || len(result.Sent) != 1 || waits != 1 {
		t.Fatalf("响应体超时也应受控重试：%+v %v 等待=%d", result, err, waits)
	}
}

// 完整 JSON 后连接仍未结束时，整个响应尚未确认；重试不得继承这些字段。
func TestCheckRetryDoesNotReuseUnconfirmedSnapshotFields(t *testing.T) {
	for _, path := range []string{"/fapi/v1/time", "/fapi/v1/exchangeInfo"} {
		t.Run(path, func(t *testing.T) {
			f := newFixture(t)
			f.add("GOODUSDT", "300", "1", "4", "10", "12")
			var attempts atomic.Int64
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == path {
					if attempts.Add(1) == 1 {
						f.server.Config.Handler.ServeHTTP(w, r)
						w.(http.Flusher).Flush()
						<-r.Context().Done()
						return
					}
					io.WriteString(w, `{}`)
					return
				}
				f.server.Config.Handler.ServeHTTP(w, r)
			}))
			defer proxy.Close()
			cfg := f.config()
			cfg.BinanceBaseURL = proxy.URL
			cfg.HTTPTimeout = 30 * time.Millisecond
			app, err := monitor.New(cfg, monitor.Options{Wait: func(context.Context, time.Duration) error { return nil }, Logger: log.New(io.Discard, "", 0)})
			if err != nil {
				t.Fatal(err)
			}
			result, err := app.Check(context.Background())
			if attempts.Load() != 2 || err == nil || result.MarketStatus != monitor.MarketFailed || len(result.Sent) != 0 || len(f.messages) != 0 {
				t.Fatalf("重试缺字段必须失败，不得通知：attempts=%d result=%+v err=%v", attempts.Load(), result, err)
			}
		})
	}
}

// 使用真实 HTTP 阻塞与请求 context 推进超时，避免逐请求累加虚拟时间掩盖调度问题。
func TestCheckSlowCandidatesLeaveTimeForHealthyContracts(t *testing.T) {
	for _, tc := range []struct {
		name         string
		healthyFirst bool
		timeout      time.Duration
		fixedClock   bool
	}{
		{"healthy_first", true, 250 * time.Millisecond, false},
		{"healthy_last", false, 250 * time.Millisecond, false},
		{"long_timeout_fixed_clock_first", true, 10 * time.Second, true},
		{"long_timeout_fixed_clock_last", false, 10 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			goodChange, badChange := "200", "400"
			if tc.healthyFirst {
				goodChange, badChange = badChange, goodChange
			}
			f.add("GOOD1USDT", goodChange, "1", "5", "10", "12")
			f.add("GOOD2USDT", goodChange, "1", "5", "10", "12")
			f.add("SLOW1USDT", badChange, "1", "5", "10", "12")
			f.add("SLOW2USDT", badChange, "1", "5", "10", "12")
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Query().Get("symbol"), "SLOW") {
					<-r.Context().Done()
					return
				}
				f.server.Config.Handler.ServeHTTP(w, r)
			}))
			defer proxy.Close()
			cfg := f.config()
			cfg.BinanceBaseURL = proxy.URL
			cfg.CheckInterval = time.Second
			cfg.HTTPTimeout = tc.timeout
			opts := monitor.Options{Wait: func(context.Context, time.Duration) error { return nil }, Logger: log.New(io.Discard, "", 0)}
			if tc.fixedClock {
				// 可控逻辑时钟停住时，真实 HTTP 截止仍须限制慢候选；不能反复重用全部份额。
				now := time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC)
				opts.Now = func() time.Time { return now }
			}
			app, err := monitor.New(cfg, opts)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			result, err := app.Check(context.Background())
			if err == nil || result.MarketStatus != monitor.MarketPartial || len(result.MarketFailures) != 2 {
				t.Fatalf("两个慢候选必须保留局部失败：%+v %v", result, err)
			}
			if len(result.Sent) != 2 || result.Sent[0].Symbol != "GOOD1USDT" || result.Sent[1].Symbol != "GOOD2USDT" || len(f.messages) != 1 {
				t.Fatalf("健康合约必须按排名合并通知，不能被慢候选耗尽有效期：%+v messages=%v err=%v", result, f.messages, err)
			}
			if elapsed := time.Since(started); elapsed >= cfg.CheckInterval {
				t.Fatalf("采集应留出投递时间：%s", elapsed)
			}
		})
	}
}
