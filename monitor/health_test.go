package monitor_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/LaoBan-ywcm/b-a/monitor"
)

func TestCheckNotifiesPersistentGlobalFailureOnceAndRecoveryOnce(t *testing.T) {
	f := newFixture(t)
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	start := now
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	f.failures["/fapi/v1/exchangeInfo"] = 400
	for _, elapsed := range []time.Duration{0, time.Minute, time.Minute + time.Nanosecond, 3 * time.Minute} {
		now = start.Add(elapsed)
		result, err := app.Check(context.Background())
		if err == nil || result.MarketStatus != monitor.MarketFailed {
			t.Fatalf("应保留全局行情错误：%+v %v", result, err)
		}
		want := 0
		if elapsed > time.Minute {
			want = 1
		}
		if len(f.messages) != want {
			t.Fatalf("超过一分钟才通知且持续失败不刷屏：elapsed=%v messages=%v", elapsed, f.messages)
		}
	}
	if !strings.Contains(f.messages[0], "行情故障") || !strings.Contains(f.messages[0], "全局") {
		t.Fatalf("故障范围不明确：%v", f.messages)
	}
	f.failures["/fapi/v1/exchangeInfo"] = 0
	for i := 0; i < 2; i++ {
		now = now.Add(time.Minute)
		result, err := app.Check(context.Background())
		if err != nil || result.MarketStatus != monitor.MarketComplete || len(result.Sent) != 0 {
			t.Fatalf("无信号属于正常检查：%+v %v", result, err)
		}
	}
	if len(f.messages) != 2 || !strings.Contains(f.messages[1], "行情恢复") || !strings.Contains(f.messages[1], "全局") {
		t.Fatalf("恢复应只通知一次：%v", f.messages)
	}
	// 同一范围再次失败属于新的事件，不能被上一次成功通知永久去重。
	f.failures["/fapi/v1/exchangeInfo"] = 400
	app.Check(context.Background())
	now = now.Add(61 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 3 {
		t.Fatalf("新故障事件应重新通知：%v", f.messages)
	}
}

func TestCheckTracksPartialScopeAcrossGlobalFailureWithoutChangingCooldown(t *testing.T) {
	f := newFixture(t)
	f.add("FAILEDUSDT", "300", "1", "4", "10", "12")
	f.add("GOODUSDT", "200", "1", "3", "10", "12")
	f.failures["/fapi/v1/klines/FAILEDUSDT"] = 400
	start := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	now := start
	opts := monitor.Options{Now: func() time.Time { return now }, Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, Logger: log.New(io.Discard, "", 0)}
	app, err := monitor.New(f.config(), opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err == nil || len(result.Sent) != 1 || result.Sent[0].Symbol != "GOODUSDT" {
		t.Fatalf("健康合约应正常通知：%+v %v", result, err)
	}
	now = start.Add(61 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 2 || !strings.Contains(f.messages[1], "行情故障") || !strings.Contains(f.messages[1], "局部合约 FAILEDUSDT") || strings.Contains(f.messages[1], "GOODUSDT") {
		t.Fatalf("局部范围必须准确：%v", f.messages)
	}
	f.failures["/fapi/v1/exchangeInfo"] = 400
	now = start.Add(121 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 2 {
		t.Fatalf("全局未知不能误报局部恢复：%v", f.messages)
	}
	now = start.Add(182 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 3 || !strings.Contains(f.messages[2], "全局") {
		t.Fatalf("应独立报告全局事件：%v", f.messages)
	}
	f.failures["/fapi/v1/exchangeInfo"] = 0
	now = start.Add(243 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 4 || !strings.Contains(f.messages[3], "行情恢复") || !strings.Contains(f.messages[3], "全局") || !strings.Contains(f.messages[3], "仍有局部故障") {
		t.Fatalf("全局恢复不能声称全部恢复：%v", f.messages)
	}
	f.failures["/fapi/v1/klines/FAILEDUSDT"] = 0
	f.tickers[0]["priceChangePercent"] = "50"
	now = start.Add(304 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 5 || !strings.Contains(f.messages[4], "行情恢复") || !strings.Contains(f.messages[4], "FAILEDUSDT") || !strings.Contains(f.messages[4], "不再需要检查") {
		t.Fatalf("退出候选的故障应解释当前范围恢复：%v", f.messages)
	}
	now = start.Add(600 * time.Second)
	result, err = app.Check(context.Background())
	if err != nil || len(result.Sent) != 1 || result.Sent[0].Symbol != "GOODUSDT" {
		t.Fatalf("健康消息不可推迟原有上涨冷却：%+v %v", result, err)
	}
	restarted, err := monitor.New(f.config(), opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 0 || len(f.messages) != 6 {
		t.Fatalf("重启仍保留上涨冷却：%+v %v 消息=%v", result, err, f.messages)
	}
}

func TestCheckReportsTelegramRecoveryWithoutSignalsOrReplayingOldAlerts(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
	f.telegramResponse = `{"ok":false,"error_code":401,"description":"TEST_SECRET"}`
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	var logs strings.Builder
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, Logger: log.New(&logs, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err == nil || len(result.Sent) != 0 || len(f.messages) != 1 {
		t.Fatalf("失败轮不能递归发送故障提醒：%+v %v messages=%v", result, err, f.messages)
	}
	if !strings.Contains(logs.String(), "Telegram 通知中断") || strings.Contains(logs.String(), "TEST_SECRET") {
		t.Fatalf("应脱敏记录中断：%s", logs.String())
	}
	// 上涨条件消失后，仍须主动检查通知链路是否恢复，不能依赖下一次上涨信号。
	f.tickers[0]["priceChangePercent"] = "10"
	now = now.Add(time.Minute)
	result, err = app.Check(context.Background())
	if err == nil || len(f.messages) != 2 || result.MarketStatus != monitor.MarketComplete {
		t.Fatalf("链路继续失败应每轮最多一次有界汇报调用：%+v %v messages=%v", result, err, f.messages)
	}
	f.telegramResponse = `{"ok":true}`
	now = now.Add(time.Minute)
	result, err = app.Check(context.Background())
	if err != nil || len(result.Sent) != 0 || len(f.messages) != 3 || !strings.Contains(f.messages[2], "Telegram 通知恢复") || strings.Contains(f.messages[2], "上涨提醒\n合约") {
		t.Fatalf("无信号时也应恢复汇报且不重放旧行情：%+v %v messages=%v", result, err, f.messages)
	}
	if !strings.Contains(logs.String(), "Telegram 通知恢复") {
		t.Fatalf("恢复应记录日志：%s", logs.String())
	}
	now = now.Add(time.Minute)
	if _, err = app.Check(context.Background()); err != nil || len(f.messages) != 3 {
		t.Fatalf("恢复不能每轮刷屏：%v %v", err, f.messages)
	}
	f.tickers[0]["priceChangePercent"] = "201"
	now = now.Add(time.Minute)
	result, err = app.Check(context.Background())
	if err != nil || len(result.Sent) != 1 {
		t.Fatalf("链路恢复消息不应让未送达合约进入冷却：%+v %v", result, err)
	}
}

func TestCheckReportsDelayedRecoveryAsPastWhenSameScopeFailsAgain(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
	f.failures["/fapi/v1/klines/ALPHAUSDT"] = 400
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	app.Check(context.Background())
	now = now.Add(61 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 1 {
		t.Fatalf("首先通知一次局部故障：%v", f.messages)
	}
	f.telegramResponse = `{"ok":false,"error_code":401}`
	f.tickers[0]["priceChangePercent"] = "10"
	now = now.Add(61 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 2 {
		t.Fatalf("健康消息失败不能递归通知：%v", f.messages)
	}
	f.tickers[0]["priceChangePercent"] = "201"
	now = now.Add(61 * time.Second)
	app.Check(context.Background())
	f.telegramResponse = `{"ok":true}`
	now = now.Add(61 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 4 {
		t.Fatalf("恢复应合并汇报而非补发队列：%v", f.messages)
	}
	text := f.messages[3]
	for _, want := range []string{"Telegram 通知恢复", "行情故障", "行情恢复", "此前事件已恢复", "当前仍有局部故障"} {
		if !strings.Contains(text, want) {
			t.Fatalf("延迟恢复不能误导当前故障状态，缺少 %q：%s", want, text)
		}
	}
}

func TestCheckBoundsCombinedHealthMessagesAndSharesAlertRateLimit(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 15; i++ {
		symbol := fmt.Sprintf("OLD%02d", i) + strings.Repeat("🚀", 100)
		f.add(symbol, "300", "1", "4", "10", "12")
		f.failures["/fapi/v1/klines/"+symbol] = 400
	}
	cfg := f.config()
	cfg.TopN = 40
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	var sentAt []time.Time
	f.onTelegram = func() { sentAt = append(sentAt, now) }
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	app.Check(context.Background())
	for i := 0; i < 15; i++ {
		symbol := fmt.Sprintf("NEW%02d", i) + strings.Repeat("🚀", 100)
		f.add(symbol, "300", "1", "4", "10", "12")
		f.failures["/fapi/v1/klines/"+symbol] = 400
	}
	now = now.Add(61 * time.Second)
	app.Check(context.Background())
	if len(f.messages) != 1 || !strings.Contains(f.messages[0], "另有 9 个范围") {
		t.Fatalf("大量故障应有界合并并说明省略范围：%v", f.messages)
	}
	for _, tick := range f.tickers[:15] {
		tick["priceChangePercent"] = "10"
	}
	f.add("GOODUSDT", "201", "1", "3.01", "10", "12")
	now = now.Add(61 * time.Second)
	result, err := app.Check(context.Background())
	if err == nil || len(result.Sent) != 1 || result.Sent[0].Symbol != "GOODUSDT" || len(f.messages) != 3 {
		t.Fatalf("健康合约与故障/恢复共存：%+v %v messages=%v", result, err, f.messages)
	}
	combined := f.messages[2]
	if !strings.Contains(combined, "行情故障") || !strings.Contains(combined, "行情恢复") || strings.Count(combined, "另有 9 个范围") != 2 {
		t.Fatalf("应合并两个有界范围摘要：%s", combined)
	}
	for _, message := range f.messages {
		if len(utf16.Encode([]rune(message))) > 4096 {
			t.Fatalf("健康通知不得超出 Telegram 长度：%d", len(utf16.Encode([]rune(message))))
		}
	}
	if sentAt[2].Sub(sentAt[1]) < time.Second {
		t.Fatalf("上涨和健康提醒必须共享速率：%v", sentAt)
	}
}

func TestCheckShortMarketInterruptionDoesNotNotify(t *testing.T) {
	f := newFixture(t)
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	f.failures["/fapi/v1/exchangeInfo"] = 400
	app.Check(context.Background())
	now = now.Add(59 * time.Second)
	f.failures["/fapi/v1/exchangeInfo"] = 0
	if result, err := app.Check(context.Background()); err != nil || result.MarketStatus != monitor.MarketComplete || len(f.messages) != 0 {
		t.Fatalf("不足门槛的故障和恢复不应通知：%+v %v messages=%v", result, err, f.messages)
	}
}

func TestRunSimulatesStartupFilteringAlertsCooldownFailureRecoveryAndRestart(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "1", "3.01", "10", "12")
	f.add("BETAUSDT", "100", "1", "2", "10", "12")
	start := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	now := start
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rounds := 0
	var sentAt []time.Time
	f.onTelegram = func() { sentAt = append(sentAt, now) }
	opts := monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(ctx context.Context, d time.Duration) error {
		if d <= time.Second {
			now = now.Add(d)
			return nil
		}
		rounds++
		wantMessages := []int{1, 1, 1, 1, 2, 4, 4}[rounds-1]
		if len(f.messages) != wantMessages {
			t.Fatalf("模拟第 %d 轮：消息=%v，期望 %d", rounds, f.messages, wantMessages)
		}
		switch rounds {
		case 2:
			f.failures["/fapi/v1/exchangeInfo"] = 400
		case 5:
			f.failures["/fapi/v1/exchangeInfo"] = 0
			f.tickers[1]["priceChangePercent"] = "150"
		case 7:
			cancel()
			return ctx.Err()
		}
		now = now.Add(d)
		return nil
	}}
	app, err := monitor.New(f.config(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = app.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.messages[0], "BETAUSDT") || !strings.Contains(f.messages[1], "行情故障") || !strings.Contains(f.messages[2], "BETAUSDT") || !strings.Contains(f.messages[3], "行情恢复") {
		t.Fatalf("模拟流程消息不符：%v", f.messages)
	}
	if sentAt[3].Sub(sentAt[2]) < time.Second {
		t.Fatalf("恢复与上涨消息未遵守共同限速：%v", sentAt)
	}
	restarted, err := monitor.New(f.config(), opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 0 {
		t.Fatalf("重启应保留两个合约的独立冷却：%+v %v", result, err)
	}
	now = start.Add(10 * time.Minute)
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 1 || result.Sent[0].Symbol != "ALPHAUSDT" {
		t.Fatalf("原冷却到期应只通知 ALPHA：%+v %v", result, err)
	}
	t.Log("模拟完成：启动立即检查 → BETA 恰好100%被筛除 → ALPHA上涨提醒 → 冷却 → 全局故障持续超过一分钟 → 故障去重 → BETA新上涨与全局恢复共享限速 → 重启保留独立冷却 → ALPHA冷却到期再次提醒")
}
