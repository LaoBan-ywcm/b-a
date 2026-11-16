package monitor_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LaoBan-ywcm/b-a/monitor"
)

// fixture 在 HTTP 边界提供行情并捕获实际发送的请求，不替换应用内部逻辑。
type fixture struct {
	server           *httptest.Server
	statePath        string
	onExchange       func()
	onTelegram       func()
	metadata         []map[string]string
	tickers          []map[string]string
	weeks            map[string][][]any
	serverTime       int64
	messages         []string
	failures         map[string]int
	telegramResponse string
	delayPath        string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{statePath: t.TempDir() + "/cooldowns.json", weeks: make(map[string][][]any), failures: make(map[string]int), serverTime: 1768176000000, telegramResponse: `{"ok":true,"result":{"message_id":1}}`}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == f.delayPath {
			select {
			case <-r.Context().Done():
			case <-time.After(200 * time.Millisecond):
			}
			return
		}
		if strings.HasPrefix(r.URL.Path, "/fapi/") && (r.Header.Get("Authorization") != "" || r.Header.Get("X-MBX-APIKEY") != "") {
			t.Error("公开行情不应携带账户凭证")
		}
		key := r.URL.Path
		if r.URL.Path == "/fapi/v1/klines" {
			key += "/" + r.URL.Query().Get("symbol")
		}
		if status := f.failures[key]; status != 0 {
			w.WriteHeader(status)
			io.WriteString(w, `{"description":"TEST_SECRET"}`)
			return
		}
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			if f.onExchange != nil {
				f.onExchange()
			}
			metadata := f.metadata
			if metadata == nil {
				metadata = []map[string]string{}
			}
			json.NewEncoder(w).Encode(map[string]any{"symbols": metadata})
		case "/fapi/v1/ticker/24hr":
			tickers := f.tickers
			if tickers == nil {
				tickers = []map[string]string{}
			}
			json.NewEncoder(w).Encode(tickers)
		case "/fapi/v1/time":
			json.NewEncoder(w).Encode(map[string]int64{"serverTime": f.serverTime})
		case "/fapi/v1/klines":
			if r.URL.Query().Get("interval") != "1w" {
				t.Errorf("周线周期 = %s", r.URL.RawQuery)
			}
			weeks := f.weeks[r.URL.Query().Get("symbol")]
			if weeks == nil {
				weeks = [][]any{}
			}
			json.NewEncoder(w).Encode(weeks)
		case "/botTEST_SECRET/sendMessage":
			var body struct {
				ChatID string `json:"chat_id"`
				Text   string `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.ChatID != "123456" {
				t.Errorf("chat_id = %q", body.ChatID)
			}
			f.messages = append(f.messages, body.Text)
			if f.onTelegram != nil {
				f.onTelegram()
			}
			io.WriteString(w, f.telegramResponse)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fixture) add(symbol, day, open, last, weekOpen, weekClose string) {
	f.metadata = append(f.metadata, map[string]string{"symbol": symbol, "status": "TRADING", "contractType": "PERPETUAL", "quoteAsset": "USDT", "marginAsset": "USDT"})
	f.tickers = append(f.tickers, map[string]string{"symbol": symbol, "priceChangePercent": day, "openPrice": open, "lastPrice": last, "quoteVolume": "0"})
	f.weeks[symbol] = [][]any{{1766966400000, weekOpen, "100", "1", weekClose, "0", 1767571199999}, {1767571200000, "10", "100", "1", "50", "0", 1768175999999}, {1768176000000, "10", "100", "1", "100", "0", 1768780799999}}
}

func (f *fixture) config() monitor.Config {
	cfg := monitor.DefaultConfig()
	cfg.StatePath = f.statePath
	cfg.BinanceBaseURL = f.server.URL
	cfg.TelegramBaseURL = f.server.URL
	cfg.TelegramToken = "TEST_SECRET"
	cfg.TelegramChatID = "123456"
	return cfg
}

func (f *fixture) check(t *testing.T, cfg monitor.Config) (monitor.Result, error) {
	t.Helper()
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return time.Date(2026, 1, 12, 1, 2, 3, 0, time.UTC) }, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	return app.Check(context.Background())
}

func TestCheckSendsVerifiableRisingAlert(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "125.5", "8", "18.04", "10", "12")
	result, err := f.check(t, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sent) != 1 || len(f.messages) != 1 {
		t.Fatalf("上涨提醒结果 = %+v, 消息 = %v", result, f.messages)
	}
	for _, want := range []string{"ALPHAUSDT", "排名：1", "24 小时涨幅：125.5%", "已收盘周线涨幅：400%", "参考起点价：8", "最新成交价：18.04", "2026-01-05T00:00:00Z", "2026-01-11T23:59:59.999Z", "2026-01-12T01:02:03Z"} {
		if !strings.Contains(f.messages[0], want) {
			t.Errorf("上涨提醒缺少 %q：%s", want, f.messages[0])
		}
	}
}

func TestCheckRanksTradingUSDTPerpetualsBeforeExclusion(t *testing.T) {
	f := newFixture(t)
	for _, item := range []struct{ symbol, day string }{{"ZUSDT", "150"}, {"BUSDT", "200"}, {"EXCLUDEDUSDT", "300"}, {"AUSDT", "200"}, {"HALTEDUSDT", "999"}, {"USDCONLY", "999"}, {"DELIVERYUSDT", "999"}, {"MARGINOTHER", "999"}} {
		f.add(item.symbol, item.day, "1", "3", "10", "12")
	}
	f.metadata[4]["status"] = "SETTLING"
	f.metadata[5]["quoteAsset"] = "USDC"
	f.metadata[6]["contractType"] = "CURRENT_QUARTER"
	f.metadata[7]["marginAsset"] = "USDC"
	cfg := f.config()
	cfg.TopN = 3
	cfg.Excluded = []string{"EXCLUDEDUSDT"}
	result, err := f.check(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sent) != 2 {
		t.Fatalf("仅前 3 名中的未排除合约应通知，实际 %+v", result.Sent)
	}
	if result.Sent[0].Symbol != "AUSDT" || result.Sent[0].Rank != 2 || result.Sent[1].Symbol != "BUSDT" || result.Sent[1].Rank != 3 {
		t.Fatalf("涨幅排名/并列顺序不正确：%+v", result.Sent)
	}
}

func TestCheckUsesExactDailyAndClosedWeeklyThresholds(t *testing.T) {
	cases := []struct {
		name, day, week string
		want            int
	}{
		{"daily_equal", "100", "12", 0},
		{"daily_below", "99.999999999999999999", "12", 0},
		{"daily_above_week_equal", "100.000000000000000001", "12", 1},
		{"week_below", "101", "11.999999999999999999", 0},
		{"week_above", "101", "12.000000000000000001", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", tc.day, "10", "21", "10", tc.week)
			f.weeks["ALPHAUSDT"][1][4] = tc.week
			result, err := f.check(t, f.config())
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Sent) != tc.want {
				t.Fatalf("发送 %d 条，期望 %d；%+v", len(result.Sent), tc.want, result)
			}
		})
	}
}

func TestCheckSkipsInvalidMarketDataWithDiagnostics(t *testing.T) {
	cases := []struct {
		name   string
		change func(*fixture)
	}{
		{"missing_daily", func(f *fixture) { f.tickers[0]["priceChangePercent"] = "" }},
		{"invalid_daily", func(f *fixture) { f.tickers[0]["priceChangePercent"] = "NaN" }},
		{"fraction_is_not_decimal", func(f *fixture) { f.tickers[0]["priceChangePercent"] = "201/1" }},
		{"zero_reference", func(f *fixture) { f.tickers[0]["openPrice"] = "0" }},
		{"negative_last", func(f *fixture) { f.tickers[0]["lastPrice"] = "-1" }},
		{"missing_last", func(f *fixture) { delete(f.tickers[0], "lastPrice") }},
		{"invalid_week_open", func(f *fixture) { f.weeks["ALPHAUSDT"][1][1] = "oops" }},
		{"zero_week_close", func(f *fixture) { f.weeks["ALPHAUSDT"][1][4] = "0" }},
		{"no_closed_week", func(f *fixture) { f.weeks["ALPHAUSDT"] = f.weeks["ALPHAUSDT"][2:] }},
		{"no_week", func(f *fixture) { f.weeks["ALPHAUSDT"] = nil }},
		{"malformed_week", func(f *fixture) { f.weeks["ALPHAUSDT"] = [][]any{{1767571200000, "10"}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
			tc.change(f)
			var logs strings.Builder
			app, err := monitor.New(f.config(), monitor.Options{Logger: log.New(&logs, "", 0)})
			if err != nil {
				t.Fatal(err)
			}
			result, err := app.Check(context.Background())
			normal := tc.name == "no_closed_week" || tc.name == "no_week"
			if normal && err != nil {
				t.Fatalf("无历史周线属于完整检查：%v", err)
			}
			if !normal && (err == nil || len(result.MarketFailures) != 1) {
				t.Fatalf("无效数据必须保留失败分类：%+v %v", result, err)
			}
			if len(result.Sent) != 0 || len(f.messages) != 0 {
				t.Fatalf("无效数据触发了上涨提醒：%+v", result)
			}
			if len(result.Diagnostics) == 0 || !strings.Contains(logs.String(), "ALPHAUSDT") {
				t.Fatalf("缺少诊断：%+v，日志 %q", result, logs.String())
			}
		})
	}
}

func TestCheckReportsPartialMarketFailureAndStillNotifiesOtherContracts(t *testing.T) {
	f := newFixture(t)
	f.add("FAILEDUSDT", "300", "1", "4", "10", "12")
	f.add("GOODUSDT", "200", "1", "3", "10", "12")
	f.failures["/fapi/v1/klines/FAILEDUSDT"] = http.StatusServiceUnavailable
	result, err := f.check(t, f.config())
	if err == nil || !strings.Contains(err.Error(), "FAILEDUSDT") {
		t.Fatalf("应报告候选周线请求失败：%v", err)
	}
	if len(result.Sent) != 1 || result.Sent[0].Symbol != "GOODUSDT" {
		t.Fatalf("完整候选应继续检查：%+v", result)
	}
	if len(result.Diagnostics) == 0 {
		t.Fatalf("缺少失败诊断：%+v", result)
	}
}

func TestNewRejectsInvalidConfigurationWithoutExposingToken(t *testing.T) {
	cases := []struct {
		name   string
		change func(*monitor.Config)
	}{
		{"no_token", func(c *monitor.Config) { c.TelegramToken = "" }},
		{"unsafe_token", func(c *monitor.Config) { c.TelegramToken = "TEST_SECRET/?" }},
		{"no_chat", func(c *monitor.Config) { c.TelegramChatID = "" }},
		{"group_not_private", func(c *monitor.Config) { c.TelegramChatID = "-123" }},
		{"zero_top", func(c *monitor.Config) { c.TopN = 0 }},
		{"negative_top", func(c *monitor.Config) { c.TopN = -1 }},
		{"bad_day", func(c *monitor.Config) { c.DayThreshold = "NaN" }},
		{"bad_week", func(c *monitor.Config) { c.WeekThreshold = "1/2" }},
		{"negative_threshold", func(c *monitor.Config) { c.DayThreshold = "-1" }},
		{"zero_interval", func(c *monitor.Config) { c.CheckInterval = 0 }},
		{"negative_interval", func(c *monitor.Config) { c.CheckInterval = -time.Second }},
		{"zero_cooldown", func(c *monitor.Config) { c.Cooldown = 0 }},
		{"negative_cooldown", func(c *monitor.Config) { c.Cooldown = -time.Second }},
		{"no_state_path", func(c *monitor.Config) { c.StatePath = " " }},
		{"no_timeout", func(c *monitor.Config) { c.HTTPTimeout = 0 }},
		{"invalid_url", func(c *monitor.Config) { c.BinanceBaseURL = ":bad" }},
		{"url_has_secret", func(c *monitor.Config) { c.TelegramBaseURL = "https://user:TEST_SECRET@example.test" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := monitor.DefaultConfig()
			cfg.TelegramToken = "TEST_SECRET"
			cfg.TelegramChatID = "123456"
			tc.change(&cfg)
			_, err := monitor.New(cfg, monitor.Options{})
			if err == nil {
				t.Fatal("非法配置未失败")
			}
			if strings.Contains(err.Error(), "TEST_SECRET") {
				t.Fatalf("错误泄露凭证：%v", err)
			}
		})
	}
}

func TestCheckRequiresValidExchangeTime(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
	f.serverTime = 0
	result, err := f.check(t, f.config())
	if err == nil || len(result.Sent) != 0 {
		t.Fatalf("缺失交易所时间不可当作成功检查：%+v %v", result, err)
	}
}

func TestCheckSelectsNewestClosedWeekAtExchangeTimeBoundary(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
	// 最新一周低于门槛；只有其收盘时间严格早于交易所时间时才替代上一根已收盘周线。
	f.weeks["ALPHAUSDT"][2][4] = "11"
	f.weeks["ALPHAUSDT"] = [][]any{f.weeks["ALPHAUSDT"][2], f.weeks["ALPHAUSDT"][0], f.weeks["ALPHAUSDT"][1]}
	for _, tc := range []struct {
		at   int64
		want int
	}{{1768780799999, 1}, {1768780800000, 0}} {
		f.serverTime = tc.at
		result, err := f.check(t, f.config())
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Sent) != tc.want {
			t.Fatalf("交易所时间 %d：结果 %+v", tc.at, result)
		}
	}
}

func TestCheckReportsTelegramRejectionAndNeverLeaksToken(t *testing.T) {
	for _, name := range []string{"http_error", "ok_false", "invalid_json", "timeout", "transport_error"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.add("ALPHAUSDT", "201", "1", "3", "10", "12")
			cfg := f.config()
			switch name {
			case "http_error":
				f.failures["/botTEST_SECRET/sendMessage"] = 401
			case "ok_false":
				f.telegramResponse = `{"ok":false,"description":"https://api.telegram.org/botTEST_SECRET/sendMessage"}`
			case "invalid_json":
				f.telegramResponse = `TEST_SECRET`
			case "timeout":
				f.delayPath = "/botTEST_SECRET/sendMessage"
				cfg.HTTPTimeout = 20 * time.Millisecond
			case "transport_error":
				cfg.TelegramBaseURL = "http://127.0.0.1:1"
			}
			var logs strings.Builder
			app, err := monitor.New(cfg, monitor.Options{Logger: log.New(&logs, "", 0)})
			if err != nil {
				t.Fatal(err)
			}
			result, err := app.Check(context.Background())
			if err == nil || len(result.Sent) != 0 || len(result.Alerts) != 1 {
				t.Fatalf("未确认成功不得计入 Sent：%+v %v", result, err)
			}
			var failure *monitor.CheckError
			if !errors.As(err, &failure) || failure.Service != "Telegram" {
				t.Fatalf("应区分通知失败：%v", err)
			}
			if strings.Contains(err.Error()+logs.String(), "TEST_SECRET") {
				t.Fatalf("凭证泄露：%v 日志 %s", err, logs.String())
			}
		})
	}
}

func TestCheckDefaultTopTenAndConfiguredThresholds(t *testing.T) {
	f := newFixture(t)
	for _, symbol := range []string{"KUSDT", "JUSDT", "IUSDT", "HUSDT", "GUSDT", "FUSDT", "EUSDT", "DUSDT", "CUSDT", "BUSDT", "AUSDT"} {
		f.add(symbol, "101", "1", "2.01", "10", "12")
	}
	cfg := f.config()
	cfg.Excluded = []string{"AUSDT"}
	result, err := f.check(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Sent) != 9 || result.Sent[8].Symbol != "JUSDT" || result.Sent[8].Rank != 10 {
		t.Fatalf("默认排名前 10，排除后不递补：%+v", result.Sent)
	}
	cfg.TopN = 1
	cfg.Excluded = nil
	cfg.DayThreshold = "100.9"
	cfg.WeekThreshold = "400.01"
	result, err = f.check(t, cfg)
	if err != nil || len(result.Sent) != 0 {
		t.Fatalf("自定义周线阈值应生效：%+v %v", result, err)
	}
	cfg.WeekThreshold = "400"
	cfg.DayThreshold = "101"
	result, err = f.check(t, cfg)
	if err != nil || len(result.Sent) != 0 {
		t.Fatalf("自定义日涨幅严格比较：%+v %v", result, err)
	}
	cfg.DayThreshold = "100.9"
	result, err = f.check(t, cfg)
	if err != nil || len(result.Sent) != 1 {
		t.Fatalf("自定义门槛通过：%+v %v", result, err)
	}
}
