package monitor_test

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LaoBan-ywcm/b-a/monitor"
)

func TestCheckCooldownIsIndependentAndExpiresExactlyAtBoundary(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	check := func(want ...string) {
		t.Helper()
		result, err := app.Check(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Sent) != len(want) {
			t.Fatalf("通知 = %+v，期望 %v", result.Sent, want)
		}
		for i, symbol := range want {
			if result.Sent[i].Symbol != symbol {
				t.Fatalf("通知 = %+v，期望 %v", result.Sent, want)
			}
		}
	}
	check("ALPHAUSDT")
	now = now.Add(5 * time.Minute)
	f.add("BETAUSDT", "150", "10", "25", "10", "12")
	check("BETAUSDT")
	now = now.Add(5*time.Minute - time.Nanosecond)
	check()
	now = now.Add(time.Nanosecond)
	check("ALPHAUSDT")
	now = now.Add(5 * time.Minute)
	check("BETAUSDT")
}

func TestCheckRestoresSuccessfulCooldownAfterRestart(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
	cfg := f.config()
	cfg.StatePath = t.TempDir() + "/state/cooldowns.json"
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	newApp := func() *monitor.Monitor {
		t.Helper()
		app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)})
		if err != nil {
			t.Fatal(err)
		}
		return app
	}
	result, err := newApp().Check(context.Background())
	if err != nil || len(result.Sent) != 1 {
		t.Fatalf("首次通知 = %+v, %v", result, err)
	}
	now = now.Add(9 * time.Minute)
	restarted := newApp()
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 0 {
		t.Fatalf("重启后应保留冷却 = %+v, %v", result, err)
	}
	now = now.Add(time.Minute)
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 1 {
		t.Fatalf("重启后到期应通知 = %+v, %v", result, err)
	}
}

func TestRunStartsImmediatelyRefreshesMarketAndSurvivesFailedRound(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	app, err := monitor.New(f.config(), monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(ctx context.Context, delay time.Duration) error {
		// 行情失败的短退避推进可控时间，但不等同于下一检查轮次。
		if delay == time.Second || delay == 2*time.Second {
			now = now.Add(delay)
			return nil
		}
		expected := time.Minute
		if waits == 1 {
			expected = 57 * time.Second
		}
		if delay != expected {
			t.Fatalf("默认检查周期剩余等待 = %v，期望 %v", delay, expected)
		}
		waits++
		switch waits {
		case 1:
			if len(f.messages) != 1 {
				t.Fatalf("启动必须立即检查，消息 = %v", f.messages)
			}
			f.failures["/fapi/v1/exchangeInfo"] = 503
		case 2:
			if len(f.messages) != 1 {
				t.Fatalf("失败轮次不应通知，消息 = %v", f.messages)
			}
			f.failures["/fapi/v1/exchangeInfo"] = 0
			f.metadata[0]["status"] = "SETTLING"
			f.add("BETAUSDT", "150", "10", "25", "10", "12")
		case 3:
			if len(f.messages) != 2 {
				t.Fatalf("恢复后应发现新合约，消息 = %v", f.messages)
			}
			f.metadata = nil
		case 4:
			if len(f.messages) != 2 {
				t.Fatalf("下架后不应通知，消息 = %v", f.messages)
			}
			cancel()
			return ctx.Err()
		}
		now = now.Add(delay)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = app.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if waits != 4 {
		t.Fatalf("执行轮次 = %d", waits)
	}
}

func TestNewReportsCorruptStateInsteadOfSendingAgain(t *testing.T) {
	for _, data := range []string{"", `TEST_SECRET`, `{}`, `null`, `{"version":2,"last_sent":{}}`, `{"version":1,"last_sent":{"ALPHAUSDT":"bad"}}`, `{"version":1,"last_sent":{"ALPHAUSDT":"0001-01-01T00:00:00Z"}}`, `{"version":1,"last_sent":{},"unexpected":true}`} {
		t.Run(data, func(t *testing.T) {
			f := newFixture(t)
			cfg := f.config()
			if err := os.WriteFile(cfg.StatePath, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := monitor.New(cfg, monitor.Options{})
			var stateErr *monitor.StateError
			if !errors.As(err, &stateErr) || strings.Contains(err.Error(), "TEST_SECRET") {
				t.Fatalf("损坏状态必须显式安全失败：%v", err)
			}
		})
	}
}

func TestStateWriteFailurePreservesPreviousStateAndStopsFurtherChecks(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
	cfg := f.config()
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	opts := monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)}
	app, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = app.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 用同目录的不可覆盖目录模拟原子写入失败；旧状态文件必须仍能完整恢复。
	if err = os.Mkdir(cfg.StatePath+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	f.add("BETAUSDT", "150", "10", "25", "10", "12")
	result, err := app.Check(context.Background())
	var stateErr *monitor.StateError
	if !errors.As(err, &stateErr) || len(result.Sent) != 1 || result.Sent[0].Symbol != "BETAUSDT" {
		t.Fatalf("已发送但保存失败 = %+v %v", result, err)
	}
	result, err = app.Check(context.Background())
	if !errors.As(err, &stateErr) || len(result.Sent) != 0 {
		t.Fatalf("状态失败后应停止发送 = %+v %v", result, err)
	}
	restarted, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatalf("原子写失败不能损坏上一份状态：%v", err)
	}
	result, err = restarted.Check(context.Background())
	if len(result.Sent) != 1 || result.Sent[0].Symbol != "BETAUSDT" || !errors.As(err, &stateErr) {
		t.Fatalf("重启应保留 ALPHA 冷却，BETA 未持久化 = %+v %v", result, err)
	}
	if err = app.Run(context.Background()); !errors.As(err, &stateErr) {
		t.Fatalf("持续服务应因状态故障退出：%v", err)
	}
}

func TestCheckFailedNotificationDoesNotConsumeCooldown(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
	f.telegramResponse = `{"ok":false}`
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	cfg := f.config()
	opts := monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)}
	app, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err == nil || len(result.Sent) != 0 {
		t.Fatalf("失败不应冷却 = %+v %v", result, err)
	}
	now = now.Add(time.Minute)
	f.telegramResponse = `{"ok":true}`
	result, err = app.Check(context.Background())
	if err != nil || len(result.Sent) != 1 {
		t.Fatalf("恢复后应立即通知 = %+v %v", result, err)
	}
	// 即使存在未完成的临时文件，也只从正式状态恢复，避免崩溃时的半成品覆盖有效状态。
	if err = os.WriteFile(cfg.StatePath+".tmp", []byte("incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	restarted, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 0 {
		t.Fatalf("仅已成功通知应从正式状态恢复 = %+v %v", result, err)
	}
}

func TestCooldownBeginsAtSuccessfulSendAndUsesConfiguredDuration(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	f.onTelegram = func() { now = now.Add(30 * time.Second) }
	cfg := f.config()
	cfg.Cooldown = 2 * time.Minute
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := app.Check(context.Background()); err != nil || len(result.Sent) != 1 {
		t.Fatalf("首次通知 = %+v %v", result, err)
	}
	f.onTelegram = nil
	now = now.Add(90 * time.Second)
	if result, err := app.Check(context.Background()); err != nil || len(result.Sent) != 0 {
		t.Fatalf("应从成功发送而非检查开始计算冷却 = %+v %v", result, err)
	}
	now = now.Add(30 * time.Second)
	if result, err := app.Check(context.Background()); err != nil || len(result.Sent) != 1 {
		t.Fatalf("自定义冷却恰好到期 = %+v %v", result, err)
	}
}

func TestRunSkipsMissedIntervalsWithoutOverlappingOrCatchingUp(t *testing.T) {
	f := newFixture(t)
	f.add("ALPHAUSDT", "201", "10", "30.1", "10", "12")
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	cfg := f.config()
	cfg.CheckInterval = 20 * time.Second
	rounds := 0
	f.onExchange = func() { rounds++; now = now.Add(55 * time.Second) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	app, err := monitor.New(cfg, monitor.Options{Now: func() time.Time { return now }, Logger: log.New(io.Discard, "", 0), Wait: func(ctx context.Context, delay time.Duration) error {
		waits++
		if delay != 5*time.Second || rounds != waits {
			t.Fatalf("慢轮次结束后应跳过错过的周期且不重叠，delay=%v rounds=%d waits=%d", delay, rounds, waits)
		}
		if waits == 2 {
			cancel()
			return ctx.Err()
		}
		now = now.Add(delay)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = app.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if rounds != 2 {
		t.Fatalf("不应补跑过时检查，实际轮次=%d", rounds)
	}
}

func TestRunCanStopWhileWaitingForNextInterval(t *testing.T) {
	f := newFixture(t)
	cfg := f.config()
	cfg.CheckInterval = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	app, err := monitor.New(cfg, monitor.Options{Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err = app.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil || time.Since(started) > time.Second {
		t.Fatal("取消应立即中断调度等待")
	}
}

func TestCheckPartialNotificationSuccessPersistsOnlyConfirmedContracts(t *testing.T) {
	f := newFixture(t)
	alpha := "ALPHA" + strings.Repeat("甲", 1900)
	beta := "BETA" + strings.Repeat("乙", 1900)
	f.add(alpha, "201", "10", "30.1", "10", "12")
	f.add(beta, "150", "10", "25", "10", "12")
	sends := 0
	f.onTelegram = func() {
		sends++
		if sends == 2 {
			f.telegramResponse = `{"ok":false}`
		}
	}
	cfg := f.config()
	now := time.Date(2026, 1, 12, 1, 0, 0, 0, time.UTC)
	opts := monitor.Options{Now: func() time.Time { return now }, Wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, Logger: log.New(io.Discard, "", 0)}
	app, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err := app.Check(context.Background())
	if err == nil || len(result.Sent) != 1 || result.Sent[0].Symbol != alpha {
		t.Fatalf("仅第一个合约成功 = %+v %v", result, err)
	}
	f.onTelegram = nil
	f.telegramResponse = `{"ok":true}`
	restarted, err := monitor.New(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	result, err = restarted.Check(context.Background())
	if err != nil || len(result.Sent) != 1 || result.Sent[0].Symbol != beta {
		t.Fatalf("重启后仅未成功的合约立即通知 = %+v %v", result, err)
	}
}
