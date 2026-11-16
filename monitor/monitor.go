package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config 使用百分数文本保存阈值，避免配置读取时引入浮点舍入。
// 调用方应从 DefaultConfig 开始，再填入运行环境提供的 Telegram 凭证。
type Config struct {
	TopN            int
	DayThreshold    string
	WeekThreshold   string
	Excluded        []string
	BinanceBaseURL  string
	TelegramBaseURL string
	TelegramToken   string
	TelegramChatID  string
	HTTPTimeout     time.Duration
	Cooldown        time.Duration
	StatePath       string
	CheckInterval   time.Duration
}

func DefaultConfig() Config {
	return Config{TopN: 10, DayThreshold: "100", WeekThreshold: "20", BinanceBaseURL: "https://fapi.binance.com", TelegramBaseURL: "https://api.telegram.org", HTTPTimeout: 10 * time.Second, Cooldown: 10 * time.Minute, StatePath: "data/cooldowns.json", CheckInterval: time.Minute}
}

// Options 仅暴露时间与日志边界；行情和通知测试通过本地 HTTP 服务完成。
type Options struct {
	Now    func() time.Time
	Wait   func(context.Context, time.Duration) error
	Logger *log.Logger
}

// Alert 保存同轮行情与判断依据，价格来自同一个 ticker 快照，周线时间使用 UTC。
type Alert struct {
	Symbol         string
	Rank           int
	Change24h      string
	WeekChange     string
	WeekOpenTime   time.Time
	WeekCloseTime  time.Time
	ReferencePrice string
	LastPrice      string
	CheckedAt      time.Time
}

type Diagnostic struct {
	Symbol  string
	Message string
}

// Result 即使在部分请求失败时也返回已完成的结果。
// Alerts 是达标合约，Sent 仅包含 Telegram 已确认成功的合约，后续冷却必须以 Sent 为准。
type Result struct {
	CheckedAt      time.Time
	Alerts         []Alert
	Sent           []Alert
	Diagnostics    []Diagnostic
	MarketStatus   MarketStatus
	MarketFailures []*CheckError
}

// CheckError 保留失败来源，持续运行入口可分别维护行情故障和通知故障；错误内容不含凭证或远端正文。
type CheckError struct {
	Service, Operation, Symbol string
	// Global 表示行情故障影响全局；即使限流由某个合约周线请求触发，也不能只归为该合约故障。
	Global bool
	Err    error
}

func (e *CheckError) Error() string {
	return fmt.Sprintf("%s %s %s: %v", e.Service, e.Operation, e.Symbol, e.Err)
}
func (e *CheckError) Unwrap() error { return e.Err }

type Monitor struct {
	config                  Config
	client                  *http.Client
	now                     func() time.Time
	wait                    func(context.Context, time.Duration) error
	logger                  *log.Logger
	lastSent                map[string]time.Time
	stateErr                error
	telegramNotBefore       time.Time
	marketNotBefore         time.Time
	marketLimitCount        uint
	marketIncidents         map[string]*marketIncident
	marketRecoveries        healthSummary
	telegramIncident        *telegramIncident
	telegramFailedThisCheck bool
}

func New(cfg Config, opts Options) (*Monitor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.BinanceBaseURL = strings.TrimRight(cfg.BinanceBaseURL, "/")
	cfg.TelegramBaseURL = strings.TrimRight(cfg.TelegramBaseURL, "/")
	cfg.Excluded = append([]string(nil), cfg.Excluded...)
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	if opts.Wait == nil {
		opts.Wait = waitContext
	}
	state, err := loadState(cfg.StatePath)
	if err != nil {
		return nil, err
	}
	return &Monitor{config: cfg, client: &http.Client{Timeout: cfg.HTTPTimeout}, now: opts.Now, wait: opts.Wait, logger: opts.Logger, lastSent: state}, nil
}

func (cfg Config) Validate() error {
	if cfg.TopN <= 0 {
		return fmt.Errorf("TopN 必须大于 0")
	}
	for name, value := range map[string]string{"DayThreshold": cfg.DayThreshold, "WeekThreshold": cfg.WeekThreshold} {
		n, ok := decimal(value)
		if !ok || n.Sign() < 0 {
			return fmt.Errorf("%s 必须是非负十进制数", name)
		}
	}
	if cfg.CheckInterval <= 0 {
		return fmt.Errorf("CheckInterval 必须大于 0")
	}
	if cfg.Cooldown <= 0 {
		return fmt.Errorf("Cooldown 必须大于 0")
	}
	if strings.TrimSpace(cfg.StatePath) == "" {
		return fmt.Errorf("StatePath 不能为空")
	}
	if cfg.HTTPTimeout <= 0 {
		return fmt.Errorf("HTTPTimeout 必须大于 0")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_:-]+$`).MatchString(cfg.TelegramToken) {
		return fmt.Errorf("TelegramToken 缺失或格式无效")
	}
	chat, err := strconv.ParseInt(cfg.TelegramChatID, 10, 64)
	if err != nil || chat <= 0 {
		return fmt.Errorf("TelegramChatID 必须是私聊的正整数 ID")
	}
	for name, base := range map[string]string{"BinanceBaseURL": cfg.BinanceBaseURL, "TelegramBaseURL": cfg.TelegramBaseURL} {
		u, err := url.Parse(base)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("%s 必须是无凭证、查询或片段的 HTTP(S) 地址", name)
		}
	}
	return nil
}

type symbolInfo struct{ Symbol, Status, ContractType, QuoteAsset, MarginAsset string }
type ticker struct{ Symbol, PriceChangePercent, OpenPrice, LastPrice string }

// Check 执行一轮检查，每次重新读取合约范围和行情；仅已确认成功的通知进入按合约计算的冷却。
// 单个周线请求失败不能阻止其他完整候选，全部失败最终使用 errors.Join 汇总。
func (m *Monitor) Check(ctx context.Context) (result Result, err error) {
	result = Result{CheckedAt: m.now()}
	m.telegramFailedThisCheck = false
	if m.stateErr != nil {
		return result, m.stateErr
	}
	defer func() {
		if ctx.Err() == nil && m.stateErr == nil {
			err = errors.Join(err, m.finishHealth(ctx, &result))
		}
	}()
	result.MarketStatus = MarketFailed
	var exchange struct{ Symbols []symbolInfo }
	if err := m.marketRequest(ctx, m.config.BinanceBaseURL+"/fapi/v1/exchangeInfo", &exchange); err != nil {
		return result, m.marketFailure(&result, "exchangeInfo", "", err, true)
	}
	var tickers []ticker
	if err := m.marketRequest(ctx, m.config.BinanceBaseURL+"/fapi/v1/ticker/24hr", &tickers); err != nil {
		return result, m.marketFailure(&result, "ticker/24hr", "", err, true)
	}
	if exchange.Symbols == nil {
		return result, m.marketFailure(&result, "exchangeInfo", "", fmt.Errorf("合约范围缺失或无效"), true)
	}
	if tickers == nil {
		return result, m.marketFailure(&result, "ticker/24hr", "", fmt.Errorf("全市场 ticker 快照缺失或无效"), true)
	}
	eligible := make(map[string]bool)
	metadataSeen := make(map[string]bool)
	for _, symbol := range exchange.Symbols {
		if symbol.Symbol == "" || symbol.Status == "" || symbol.ContractType == "" || symbol.QuoteAsset == "" || symbol.MarginAsset == "" || metadataSeen[symbol.Symbol] {
			return result, m.marketFailure(&result, "exchangeInfo", "", fmt.Errorf("合约范围字段缺失或重复"), true)
		}
		metadataSeen[symbol.Symbol] = true
		if symbol.Status == "TRADING" && symbol.ContractType == "PERPETUAL" && symbol.QuoteAsset == "USDT" && symbol.MarginAsset == "USDT" {
			eligible[symbol.Symbol] = true
		}
	}
	ranked := make([]ticker, 0, len(tickers))
	seen := make(map[string]bool)
	for _, tick := range tickers {
		if !eligible[tick.Symbol] {
			continue
		}
		if seen[tick.Symbol] {
			return result, m.marketFailure(&result, "ticker/24hr", tick.Symbol, fmt.Errorf("重复的 ticker 合约"), true)
		}
		seen[tick.Symbol] = true
		if _, ok := decimal(tick.PriceChangePercent); !ok {
			return result, m.marketFailure(&result, "ticker/24hr", tick.Symbol, fmt.Errorf("24 小时涨幅缺失或无效，无法确认排名"), true)
		}
		ranked = append(ranked, tick)
	}
	for symbol := range eligible {
		if !seen[symbol] {
			return result, m.marketFailure(&result, "ticker/24hr", symbol, fmt.Errorf("监控合约缺少 ticker，无法确认排名"), true)
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		a, _ := new(big.Rat).SetString(ranked[i].PriceChangePercent)
		b, _ := new(big.Rat).SetString(ranked[j].PriceChangePercent)
		comparison := a.Cmp(b)
		if comparison == 0 {
			return ranked[i].Symbol < ranked[j].Symbol
		}
		return comparison > 0
	})
	if len(ranked) > m.config.TopN {
		ranked = ranked[:m.config.TopN]
	}
	// 先截取排名再应用排除列表，保留真实名次，也保证排除后不会由榜外合约递补。
	excluded := make(map[string]bool)
	for _, symbol := range m.config.Excluded {
		excluded[symbol] = true
	}
	var server struct{ ServerTime int64 }
	if err := m.marketRequest(ctx, m.config.BinanceBaseURL+"/fapi/v1/time", &server); err != nil {
		return result, m.marketFailure(&result, "time", "", err, true)
	}
	if server.ServerTime <= 0 {
		return result, m.marketFailure(&result, "time", "", fmt.Errorf("交易所时间缺失或无效"), true)
	}
	result.MarketStatus = MarketComplete
	var failures []error
	var pending []Alert
	type candidate struct {
		rank   int
		ticker ticker
	}
	var candidates []candidate
	for i, ticker := range ranked {
		if excluded[ticker.Symbol] {
			continue
		}
		if !positivePrice(ticker.OpenPrice) || !positivePrice(ticker.LastPrice) {
			failures = append(failures, m.marketFailure(&result, "ticker/24hr", ticker.Symbol, fmt.Errorf("ticker 参考起点价或最新成交价缺失、无效或非正"), false))
			continue
		}
		daily, _ := new(big.Rat).SetString(ticker.PriceChangePercent)
		dayThreshold, _ := new(big.Rat).SetString(m.config.DayThreshold)
		if daily.Cmp(dayThreshold) <= 0 {
			continue
		}
		candidates = append(candidates, candidate{rank: i + 1, ticker: ticker})
	}
	// 周线只使用剩余信号有效期的一半，为排序合并、发送和限速保留另一半。
	// 按剩余候选均分剩余预算；慢候选只能耗尽自己的份额，后排健康合约仍有请求机会。
	// 不主动等待合并：所有候选完成（或耗尽份额）后立即发送，不延长原有信号 TTL。
	collectionStarted := m.now()
	wallStarted := time.Now()
	collectionBudget := max(time.Duration(0), result.CheckedAt.Add(min(m.config.CheckInterval, time.Minute)).Sub(collectionStarted)/2)
	for i, candidate := range candidates {
		ticker := candidate.ticker
		// 注入时钟控制逻辑有效期，真实经过时间约束实际 HTTP；两者任一推进都消耗预算。
		elapsed := max(m.now().Sub(collectionStarted), time.Since(wallStarted))
		budget := max(time.Duration(0), collectionBudget-elapsed) / time.Duration(len(candidates)-i)
		requestCtx, cancel := context.WithTimeout(ctx, budget)
		var rows [][]json.RawMessage
		endpoint := m.config.BinanceBaseURL + "/fapi/v1/klines?interval=1w&limit=3&symbol=" + url.QueryEscape(ticker.Symbol)
		requestErr := m.marketRequest(requestCtx, endpoint, &rows)
		cancel()
		if err := requestErr; err != nil {
			failure := m.marketFailure(&result, "klines", ticker.Symbol, err, false)
			failures = append(failures, failure)
			continue
		}
		if rows == nil {
			failures = append(failures, m.marketFailure(&result, "klines", ticker.Symbol, fmt.Errorf("周线快照缺失或无效"), false))
			continue
		}
		var selected []json.RawMessage
		var latest int64 = -1
		malformed := false
		// 不依赖响应数组顺序或自行推算周边界；收盘毫秒等于交易所时间时仍未结束。
		// 若时间结构损坏，整份周线快照不可确认，不能退回旧周线制造错误提醒。
		for _, row := range rows {
			if len(row) < 7 {
				malformed = true
				break
			}
			var openTime, closeTime int64
			if json.Unmarshal(row[0], &openTime) != nil || json.Unmarshal(row[6], &closeTime) != nil || openTime < 0 || closeTime < openTime {
				malformed = true
				break
			}
			if closeTime < server.ServerTime && openTime > latest {
				selected = row
				latest = openTime
			}
		}
		if malformed {
			failures = append(failures, m.marketFailure(&result, "klines", ticker.Symbol, fmt.Errorf("周线时间或结构无效"), false))
			continue
		}
		if selected == nil {
			m.diagnose(&result, ticker.Symbol, "没有已收盘周线")
			continue
		}
		var open, close string
		var closeTime int64
		json.Unmarshal(selected[1], &open)
		json.Unmarshal(selected[4], &close)
		json.Unmarshal(selected[6], &closeTime)
		openPrice, ok := decimal(open)
		if !ok || openPrice.Sign() <= 0 {
			failures = append(failures, m.marketFailure(&result, "klines", ticker.Symbol, fmt.Errorf("周线开盘价缺失、无效或非正"), false))
			continue
		}
		closePrice, ok := decimal(close)
		if !ok || closePrice.Sign() <= 0 {
			failures = append(failures, m.marketFailure(&result, "klines", ticker.Symbol, fmt.Errorf("周线收盘价缺失、无效或非正"), false))
			continue
		}
		change := new(big.Rat).Mul(new(big.Rat).Sub(new(big.Rat).Quo(closePrice, openPrice), big.NewRat(1, 1)), big.NewRat(100, 1))
		// 阈值比较始终使用精确有理数；仅展示时舍入，避免浮点误差改变 100%/20% 边界。
		weekThreshold, _ := new(big.Rat).SetString(m.config.WeekThreshold)
		if change.Cmp(weekThreshold) < 0 {
			continue
		}
		alert := Alert{Symbol: ticker.Symbol, Rank: candidate.rank, Change24h: ticker.PriceChangePercent, WeekChange: decimalText(change), WeekOpenTime: time.UnixMilli(latest).UTC(), WeekCloseTime: time.UnixMilli(closeTime).UTC(), ReferencePrice: ticker.OpenPrice, LastPrice: ticker.LastPrice, CheckedAt: result.CheckedAt}
		result.Alerts = append(result.Alerts, alert)
		if last, ok := m.lastSent[alert.Symbol]; ok && m.now().Sub(last) < m.config.Cooldown {
			continue
		}
		pending = append(pending, alert)
	}
	if len(pending) > 0 {
		if err := m.deliverAlerts(ctx, &result, pending); err != nil {
			failures = append(failures, err)
		}
	}
	return result, errors.Join(failures...)
}

func decimalText(n *big.Rat) string {
	return strings.TrimRight(strings.TrimRight(n.FloatString(12), "0"), ".")
}

var decimalPattern = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

// Binance 的价格字段是十进制字符串；拒绝 Rat 额外支持的分数和进制语法，避免把异常数据视为行情。
func decimal(value string) (*big.Rat, bool) {
	if len(value) > 256 || !decimalPattern.MatchString(value) {
		return nil, false
	}
	return new(big.Rat).SetString(value)
}

func positivePrice(value string) bool { n, ok := decimal(value); return ok && n.Sign() > 0 }

func (m *Monitor) diagnose(result *Result, symbol, message string) {
	result.Diagnostics = append(result.Diagnostics, Diagnostic{Symbol: symbol, Message: message})
	m.logger.Printf("%s: %s", symbol, message)
}
