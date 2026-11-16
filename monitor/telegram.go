package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

func alertText(alert Alert) string {
	return fmt.Sprintf("上涨提醒\n合约：%s\n涨幅排名：%d\n24 小时涨幅：%s%%\n已收盘周线涨幅：%s%%\n周线区间：%s 至 %s\n24 小时参考起点价：%s\n最新成交价：%s\n检查时间：%s", alert.Symbol, alert.Rank, alert.Change24h, alert.WeekChange, alert.WeekOpenTime.Format(time.RFC3339Nano), alert.WeekCloseTime.Format(time.RFC3339Nano), alert.ReferencePrice, alert.LastPrice, alert.CheckedAt.Format(time.RFC3339Nano))
}

// deliverAlerts 按完整合约块拆分纯文本；采用 UTF-16 长度的保守上限，避免 emoji 占两个编码单元时越界。
// 同轮结果已完成排序，立即合并即可，主动合并等待为零；每批确认后独立保存，后续批次失败不会回滚成功冷却。
func (m *Monitor) deliverAlerts(ctx context.Context, result *Result, alerts []Alert) error {
	for len(alerts) > 0 {
		var blocks []string
		size := 0
		count := 0
		for _, alert := range alerts {
			block := alertText(alert)
			units := len(utf16.Encode([]rune(block)))
			if units > 4096 {
				break
			}
			extra := units
			if count > 0 {
				extra += 2
			}
			if size+extra > 4096 {
				break
			}
			blocks = append(blocks, block)
			size += extra
			count++
		}
		if count == 0 {
			return &CheckError{Service: "Telegram", Operation: "sendMessage", Err: fmt.Errorf("单个合约结果超出消息长度限制")}
		}
		if err := m.sendTelegram(ctx, strings.Join(blocks, "\n\n"), result.CheckedAt.Add(min(m.config.CheckInterval, time.Minute))); err != nil {
			failure := &CheckError{Service: "Telegram", Operation: "sendMessage", Err: err}
			m.diagnose(result, "", failure.Error())
			return failure
		}
		sentAt := m.now()
		for _, alert := range alerts[:count] {
			result.Sent = append(result.Sent, alert)
			m.lastSent[alert.Symbol] = sentAt
		}
		if err := m.saveState(); err != nil {
			m.stateErr = err
			return err
		}
		alerts = alerts[count:]
	}
	return nil
}

// sendTelegram 是所有 Telegram 消息共用的私聊速率入口；后续故障/恢复提醒也应通过此入口发送。
// 重试只保留在当前调用中，跨调用只保留限速时刻，避免积压历史上涨提醒。
func (m *Monitor) sendTelegram(ctx context.Context, text string, expires time.Time) (sendErr error) {
	requestFailed := false
	started := m.now()
	// 只有真正尝试投递且最终失败才记为通知中断；过时行情和主动停止不冒充链路故障。
	// 恢复汇报由本轮结束时统一处理，发送入口本身绝不递归发送通知。
	defer func() {
		if sendErr != nil && requestFailed && ctx.Err() == nil {
			m.telegramFailedThisCheck = true
			if m.telegramIncident == nil {
				m.telegramIncident = &telegramIncident{since: started}
			}
			m.telegramIncident.failures++
			m.logger.Printf("Telegram 通知中断：开始 %s，失败投递 %d 次：%v", m.telegramIncident.since.Format(time.RFC3339Nano), m.telegramIncident.failures, sendErr)
		}
	}()
	body, _ := json.Marshal(map[string]string{"chat_id": m.config.TelegramChatID, "text": text})
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !m.now().Before(expires) || !m.telegramNotBefore.Before(expires) {
			return fmt.Errorf("Telegram 通知已过期或限流等待超出有效期")
		}
		if delay := m.telegramNotBefore.Sub(m.now()); delay > 0 {
			if err := m.wait(ctx, delay); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !m.now().Before(expires) {
			return fmt.Errorf("Telegram 通知已过期")
		}
		// 请求本身也受剩余有效期约束，避免超长 HTTP_TIMEOUT 让旧信号继续阻塞后续检查。
		requestCtx, cancel := context.WithTimeout(ctx, expires.Sub(m.now()))
		retry, after, err := m.telegramRequest(requestCtx, body)
		cancel()
		m.telegramNotBefore = m.now().Add(time.Second)
		if err == nil {
			return nil
		}
		requestFailed = true
		lastErr = err
		if !retry {
			return err
		}
		backoff := time.Second * time.Duration(1<<attempt)
		if after > backoff {
			backoff = after
		}
		// 即便已耗尽尝试次数，服务端要求的禁发期仍须保留，下一轮和其他通知同样受约束。
		m.telegramNotBefore = m.now().Add(backoff)
	}
	return lastErr
}

// telegramRequest 只返回固定诊断与状态码；URL、远端 description 和传输错误可能包含 Token，不能直接输出。
func (m *Monitor) telegramRequest(ctx context.Context, body []byte) (bool, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.config.TelegramBaseURL+"/bot"+m.config.TelegramToken+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		return false, 0, fmt.Errorf("无法创建 Telegram 请求")
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := m.client.Do(req)
	if err != nil {
		return true, 0, fmt.Errorf("Telegram 请求失败或超时，发送结果未知")
	}
	defer response.Body.Close()
	var payload struct {
		OK         bool `json:"ok"`
		ErrorCode  int  `json:"error_code"`
		Parameters struct {
			RetryAfter int64 `json:"retry_after"`
		} `json:"parameters"`
	}
	decodeErr := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload)
	after := retrySeconds(payload.Parameters.RetryAfter)
	if seconds, err := strconv.ParseInt(response.Header.Get("Retry-After"), 10, 64); err == nil {
		if wait := retrySeconds(seconds); wait > after {
			after = wait
		}
	}
	code := response.StatusCode
	if code >= 200 && code < 300 {
		if decodeErr != nil {
			return true, 0, fmt.Errorf("Telegram 响应 JSON 无效，发送结果未知")
		}
		if payload.OK {
			return false, 0, nil
		}
		code = payload.ErrorCode
	}
	retry := code == 429 || code == 408 || code >= 500
	return retry, after, fmt.Errorf("Telegram 未确认成功，状态 %d", code)
}

// 防止不可信 retry_after 在转为 Duration 时溢出，最长等待取 Duration 可表达的上限。
func retrySeconds(seconds int64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	const max = time.Duration(1<<63 - 1)
	if seconds > int64(max/time.Second) {
		return max
	}
	return time.Duration(seconds) * time.Second
}
