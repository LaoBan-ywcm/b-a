package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// marketRequest 只用于公开行情 GET；一次请求最多尝试三次，短暂故障按 1、2 秒退避。
// 响应正文和请求 URL 不进入错误信息，避免远端反射配置中的敏感信息。
func (m *Monitor) marketRequest(ctx context.Context, endpoint string, dest any) error {
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if m.now().Before(m.marketNotBefore) {
			return &MarketRateLimitError{NotBefore: m.marketNotBefore}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return fmt.Errorf("无法创建行情 HTTP 请求")
		}
		response, err := m.client.Do(req)
		retry := err != nil
		if err != nil {
			err = fmt.Errorf("行情 HTTP 请求失败或超时")
		} else {
			if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusTeapot || ((response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout) && response.Header.Get("Retry-After") != "") {
				m.recordMarketLimit(response)
				err = &MarketRateLimitError{StatusCode: response.StatusCode, NotBefore: m.marketNotBefore}
			} else {
				err, retry = decodeMarketResponse(response, dest)
				if err == nil {
					m.marketLimitCount = 0
				}
			}
			response.Body.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil || !retry || attempt == 2 {
			return err
		}
		// 时间份额不足以完成退避时立即结束，不能让某个候选的重试侵占后续候选预算。
		delay := time.Second << attempt
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
			return errors.Join(err, fmt.Errorf("行情重试超出剩余时间预算: %w", context.DeadlineExceeded))
		}
		if err := m.wait(ctx, delay); err != nil {
			return err
		}
	}
	return fmt.Errorf("行情请求重试耗尽")
}

func decodeMarketResponse(response *http.Response, dest any) (error, bool) {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("行情 HTTP 状态 %d", response.StatusCode), response.StatusCode >= 500 || response.StatusCode == http.StatusRequestTimeout
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 8<<20))
	// 先保留本次响应的原始 JSON；只有确认正文结束后才写入调用方目标。
	// 否则尾部读取超时会留下字段，下一次缺字段的响应可能沿用未确认的旧值。
	var snapshot json.RawMessage
	if err := decoder.Decode(&snapshot); err != nil {
		var networkError net.Error
		retry := errors.As(err, &networkError) || errors.Is(err, io.ErrUnexpectedEOF)
		return fmt.Errorf("行情 HTTP 响应 JSON 无效或读取中断"), retry
	}
	// 多个 JSON 值或被截断的尾部不能伪装成完整快照。
	if err := decoder.Decode(new(any)); err != io.EOF {
		var networkError net.Error
		return fmt.Errorf("行情 HTTP 响应包含额外或无效数据"), errors.As(err, &networkError)
	}
	if err := json.Unmarshal(snapshot, dest); err != nil {
		return fmt.Errorf("行情 HTTP 响应 JSON 字段无效"), false
	}
	return nil, false
}

// MarketRateLimitError 同时表示服务端限流与尚未到期的共享闸门。
// NotBefore 跨同一实例的所有行情请求和检查轮次保留；不在等待后继续旧快照。
type MarketRateLimitError struct {
	StatusCode int
	NotBefore  time.Time
}

func (e *MarketRateLimitError) Error() string {
	return fmt.Sprintf("行情限流，最早恢复时间 %s（HTTP %d）", e.NotBefore.UTC().Format(time.RFC3339Nano), e.StatusCode)
}

func (m *Monitor) recordMarketLimit(response *http.Response) {
	now := m.now()
	// Futures 未保证返回 Retry-After。缺失或无效时采用保守等待；连续封禁逐次加倍，最大一天。
	delay := time.Minute
	if response.StatusCode == http.StatusTeapot {
		delay = 2 * time.Minute
	}
	for i := uint(0); i < m.marketLimitCount && delay < 24*time.Hour; i++ {
		delay *= 2
	}
	if delay > 24*time.Hour {
		delay = 24 * time.Hour
	}
	m.marketLimitCount++
	header := strings.TrimSpace(response.Header.Get("Retry-After"))
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil && seconds > 0 && seconds <= int64((1<<63-1)/int64(time.Second)) {
		delay = time.Duration(seconds) * time.Second
	} else if deadline, err := http.ParseTime(header); err == nil && deadline.After(now) {
		delay = deadline.Sub(now)
	}
	deadline := now.Add(delay)
	if deadline.After(m.marketNotBefore) {
		m.marketNotBefore = deadline
	}
}

// MarketStatus 描述本轮行情检查覆盖程度，与 Telegram 是否成功投递无关。
// 全局范围、排名或时间无法确认时为 failed；已取得全局快照但候选未知时为 partial。
// 没有已收盘周线、未达阈值和提醒冷却均为正常的 complete；空值表示尚未开始行情检查。
type MarketStatus string

const (
	MarketComplete MarketStatus = "complete"
	MarketPartial  MarketStatus = "partial"
	MarketFailed   MarketStatus = "failed"
)

func (m *Monitor) marketFailure(result *Result, operation, symbol string, err error, global bool) *CheckError {
	var limited *MarketRateLimitError
	failure := &CheckError{Service: "Binance", Operation: operation, Symbol: symbol, Err: err, Global: global || errors.As(err, &limited)}
	result.MarketFailures = append(result.MarketFailures, failure)
	result.MarketStatus = MarketPartial
	if global {
		result.MarketStatus = MarketFailed
	}
	m.diagnose(result, symbol, failure.Error())
	return failure
}
