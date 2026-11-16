package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/LaoBan-ywcm/b-a/monitor"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run 作为命令入口边界接收环境和输出流；上下文取消用于优雅结束持续监控。
func run(ctx context.Context, getenv func(string) string, stdout, stderr io.Writer) int {
	cfg, err := configFromEnv(getenv)
	if err != nil {
		fmt.Fprintln(stderr, "配置错误：", err)
		return 1
	}
	logger := log.New(stderr, "", log.LstdFlags)
	app, err := monitor.New(cfg, monitor.Options{Logger: logger})
	if err != nil {
		fmt.Fprintln(stderr, "配置错误：", err)
		return 1
	}
	fmt.Fprintln(stdout, "监控已启动；收到 SIGINT 或 SIGTERM 后停止")
	if err := app.Run(ctx); err != nil {
		logger.Printf("监控停止：%v", err)
		return 1
	}
	return 0
}

func configFromEnv(getenv func(string) string) (monitor.Config, error) {
	cfg := monitor.DefaultConfig()
	cfg.TelegramToken = getenv("TELEGRAM_BOT_TOKEN")
	cfg.TelegramChatID = getenv("TELEGRAM_CHAT_ID")
	for key, dest := range map[string]*string{"BINANCE_BASE_URL": &cfg.BinanceBaseURL, "TELEGRAM_BASE_URL": &cfg.TelegramBaseURL, "DAILY_THRESHOLD_PCT": &cfg.DayThreshold, "WEEKLY_THRESHOLD_PCT": &cfg.WeekThreshold, "STATE_PATH": &cfg.StatePath} {
		if value := getenv(key); value != "" {
			*dest = value
		}
	}
	if value := getenv("TOP_N"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil {
			return cfg, fmt.Errorf("TOP_N 必须是整数")
		}
		cfg.TopN = n
	}
	for key, dest := range map[string]*time.Duration{"HTTP_TIMEOUT": &cfg.HTTPTimeout, "CHECK_INTERVAL": &cfg.CheckInterval, "COOLDOWN": &cfg.Cooldown} {
		if value := getenv(key); value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil {
				return cfg, fmt.Errorf("%s 必须是时间长度，例如 10s 或 10m", key)
			}
			*dest = duration
		}
	}
	for _, symbol := range strings.Split(getenv("EXCLUDED_SYMBOLS"), ",") {
		if symbol = strings.TrimSpace(symbol); symbol != "" {
			cfg.Excluded = append(cfg.Excluded, strings.ToUpper(symbol))
		}
	}
	return cfg, nil
}
