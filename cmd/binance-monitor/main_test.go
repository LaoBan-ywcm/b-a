package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunAcceptsEnvironmentOverridesAndStopsGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var rounds int
	var sent bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			rounds++
			if rounds == 2 {
				cancel()
			}
			io.WriteString(w, `{"symbols":[{"symbol":"ALPHAUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","marginAsset":"USDT"}]}`)
		case "/fapi/v1/ticker/24hr":
			io.WriteString(w, `[{"symbol":"ALPHAUSDT","priceChangePercent":"50","openPrice":"1","lastPrice":"1.5"}]`)
		case "/fapi/v1/time":
			io.WriteString(w, `{"serverTime":1768176000000}`)
		case "/fapi/v1/klines":
			io.WriteString(w, `[[1767571200000,"10","12","10","11","0",1768175999999]]`)
		case "/botTEST_SECRET/sendMessage":
			sent = true
			io.WriteString(w, `{"ok":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	// 周期同时限制行情有效期；留出真实本机 HTTP 往返与 race 插桩耗时，避免 5ms 人为制造过期。
	env := map[string]string{"TELEGRAM_BOT_TOKEN": "TEST_SECRET", "TELEGRAM_CHAT_ID": "123456", "BINANCE_BASE_URL": server.URL, "TELEGRAM_BASE_URL": server.URL, "TOP_N": "2", "DAILY_THRESHOLD_PCT": "40", "WEEKLY_THRESHOLD_PCT": "10", "EXCLUDED_SYMBOLS": "OTHERUSDT, IGNOREUSDT", "HTTP_TIMEOUT": "2s", "CHECK_INTERVAL": "1s", "COOLDOWN": "2m", "STATE_PATH": t.TempDir() + "/cooldowns.json"}
	var out, errOut bytes.Buffer
	code := run(ctx, func(key string) string { return env[key] }, &out, &errOut)
	if code != 0 || !sent || ctx.Err() == nil || !strings.Contains(errOut.String(), "成功通知 1") {
		t.Fatalf("exit=%d, sent=%v, stdout=%s stderr=%s", code, sent, out.String(), errOut.String())
	}
}

func TestRunFailsClearlyForInvalidConfiguration(t *testing.T) {
	for _, item := range []struct{ key, value string }{{"TOP_N", "oops"}, {"TOP_N", "0"}, {"HTTP_TIMEOUT", "never"}, {"CHECK_INTERVAL", "never"}, {"CHECK_INTERVAL", "0s"}, {"COOLDOWN", "never"}, {"COOLDOWN", "-1s"}, {"STATE_PATH", " "}, {"DAILY_THRESHOLD_PCT", "NaN"}, {"TELEGRAM_CHAT_ID", ""}} {
		t.Run(item.key+item.value, func(t *testing.T) {
			env := map[string]string{"TELEGRAM_BOT_TOKEN": "TEST_SECRET", "TELEGRAM_CHAT_ID": "123456", "BINANCE_BASE_URL": "http://127.0.0.1:1", "TELEGRAM_BASE_URL": "http://127.0.0.1:1"}
			env[item.key] = item.value
			var out, errOut bytes.Buffer
			code := run(context.Background(), func(key string) string { return env[key] }, &out, &errOut)
			if code == 0 || !strings.Contains(errOut.String(), "配置错误") || strings.Contains(errOut.String(), "TEST_SECRET") {
				t.Fatalf("exit=%d stderr=%s", code, errOut.String())
			}
		})
	}
}
